package com.ivalicemud.et;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.command.Command;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerLoginEvent;

import org.bukkit.plugin.java.JavaPlugin;


public class EasyTickets extends JavaPlugin implements Listener
{
	
    public static EasyTickets plugin;
	Connection conn;
	
	public static String dbprefix = "";
	public static String dbname = "";
	
    public EasyTickets()
    {
    }

    public void onEnable()
    {
    	 plugin = this;
    	 LoadConfig();
    	 dbprefix = getConfig().getString("database.prefix");
    	 dbname = dbprefix + "tickets";
    	 OpenDatabase();

         getServer().getPluginManager().registerEvents(plugin, this);
         
         purgeTickets(true);
         
         if ( getConfig().getBoolean("main.openReminder") == true )
         {
        	 plugin.getServer().getScheduler()
				.scheduleSyncRepeatingTask(plugin, new Runnable() {

					public void run() {
						ticketReminder(null,true,true);
						debug("Ticket reminder firing");
					}
				}, 30, plugin.getConfig().getInt("main.openReminderTimer") * 1200);

         }
    }
    
    public void onDisable()
    {
    	CloseDatabase();
    }
    
    public void LoadConfig()
    {
    	setConfig("main.debug",false);
    	setConfig("main.maxTickets",5);
    	setConfig("main.openReminder",true);
    	setConfig("main.openReminderTimer",10);
    	setConfig("main.loginReminder",true);
    	setConfig("main.loginReminderDelay",10);
    	setConfig("main.maxLevel",2);
    	setConfig("main.autoDeleteDays",7);
    	
    	setConfig("main.autoTeleport",true);
    	setConfig("main.allowUnassignedReply",false);
    	setConfig("tickets.quickOpen",false);
    	setConfig("tickets.listSyntax","%num%) %status% [%level%] %claimed% %text%");
    	
    	setConfig("level.1","Moderator");
    	setConfig("level.2","Admin");
    	setConfig("level.3","Owner");
    	
    	setConfig("database.useSQLite",true);
    	setConfig("database.useMySQL",false);
    	setConfig("database.dbname","DBName");
    	setConfig("database.host","localhost");
    	setConfig("database.port",0);
    	setConfig("database.user", "yourLogin");
    	setConfig("database.password","p@ssw0rd");
    	setConfig("database.prefix","easyticket_");
    	
    	saveConfig();
    }
    
    
    public void msg(CommandSender s, Object msg )
    {
    	s.sendMessage( msg.toString().replaceAll("&([0-9A-Fa-f])", "\u00A7$1") );
    }
    public void error(Object msg) {
		String txt = "[ET Error] " + msg.toString();
		Bukkit.getServer().getLogger().info( txt.replaceAll("&([0-9A-Fa-f])", "\u00A7$1") );

	}

    public void info(Object msg) {
		String txt = "[ET] " + msg.toString();
		Bukkit.getServer().getLogger().info( txt.replaceAll("&([0-9A-Fa-f])", "\u00A7$1") );

	}

	public void debug(Object msg) {
		if (plugin.getConfig().getBoolean("main.debug") == true) {
			String txt = "[ET Debug] " + msg.toString();
			Bukkit.getServer().getLogger().info( txt.replaceAll("&([0-9A-Fa-f])", "\u00A7$1") );
		}
	}
	
    public void setConfig(String line, Object set )
    {
    	if ( !getConfig().contains(line) ) getConfig().set(line,set);
    }
    
    public void purgeTickets( CommandSender s, ArrayList<String> a)
    {
    	int rank = getLevel(s);

    	if ( rank <= 0 )
    		return;
    	
    	msg(s,"&9[&FET&9]&F Purging all closed tickets....");
    	
    	purgeTickets(false);
    }
    
	public void purgeTickets( boolean auto )
    {
		int purge = getConfig().getInt("main.autoDeleteDays");
		long exp = purge * 86400000;
		long time = System.currentTimeMillis();
		long closed = 0;

    	if ( auto == false )
    	{
    		purge = 0;
    		exp = 0;
    	}
    	
		ResultSet rs = queryDB("SELECT * FROM " + dbname + " WHERE status = 5");
    	try {
    		if ( rs == null )
    			return;
			while(rs.next()) {
				int ticket = rs.getInt("id");
				closed = rs.getLong("closed");
				if ( closed+exp <= time )
				{
					debug("Ticket "+ticket + " auto deleted");
					writeDB("DELETE FROM " + dbname + " WHERE id="+ticket);
				}
				
			}
			rs.close();
		} catch (SQLException e) {
			error("purgeTickets: SQL Error");
			e.printStackTrace();
		}
    	
		renumberTickets();

    }
	
	public void renumberTickets()
	{
		
		if (!getConfig().getBoolean("database.useMySQL")) return;
		
		int num;
		ResultSet rs;
		rs = queryDB("SELECT MAX(id) AS id_ FROM "+dbname);
			
		try {
			rs.next();
			num = rs.getInt("id_");
			
			debug("max num:" + num);

		if (getConfig().getBoolean("database.useMySQL")) {
			writeDB("ALTER TABLE "+dbname+" AUTO_INCREMENT = "+num+";");
			rs.close();
		}
		else
		{
				//writeDB("UPDATE SQLITE_SEQUENCE SET seq =" +num+" WHERE name = '"+dbname+"'");
				//rs.close();
		}
			} catch (SQLException e) {
				e.printStackTrace();
				error("renumberTickets - SQL failure");
			}
			
}
		
		
	
	public void ticketReminder(Player p, boolean all, boolean auto) {
		ResultSet rs = null;
		int rank = 0;
		if ( p != null ) rank = getLevel(p);
		int open = 0, closed = 0, waiting = 0, userwait = 0, progress = 0;
		String text = "";

		if (all == false) {
			rs = queryDB("SELECT * FROM " + dbname + " WHERE who='"
					+ p.getName() + "' OR level <= " + rank);

		}
		else {

				rs = queryDB("SELECT * FROM " + dbname + " WHERE status != 5");
			}


		if (rs == null) {
			debug("No tickets found");
			return;
		}
		
			try {

				while (rs.next()) {

					switch (rs.getInt("status")) {
					case 1:
						open++;
						break;
					case 5:
						if (rs.getString("who").equals(p.getName()))
							closed++;
						break;
					case 3:
						waiting++;
						userwait++;
						break;
					case 6:
						progress++;
						break;
						
					default:
						waiting++;
						break;
					}

				}
				rs.close();
			} catch (SQLException e) {
				error("ticketReminder : SQL Error");
			}

			if ( auto == true )
			{
				debug("Checking auto tickets...");
				if ( open == 0 && userwait == 0 && progress == 0) 
					{
					debug("Auto: No tickets found");
					return;
					
					}
				text = "&9[&FET&9]&F There are tickets waiting for review:\n&2Open: "
						+ open
						+ " &6Waiting: "
						+ userwait;
				
				for (Player p2 : getServer().getOnlinePlayers()) {

					rank = getLevel(p2);

					if (rank >= 1 )
						msg(p2, text);
				}
				return;
			}
			else if ( rank <= 0 )
			{
				if (open == 0 && closed == 0 && waiting == 0 && progress == 0)
					return;
				
				text = "&9[&FET&9]&F There are tickets awaiting your review:\n&2Open: "
						+ open
						+ " &6Waiting Response: "
						+ waiting
						+ " &cClosed: "
						+ closed
						+ "\n&FUse &6/et list &7and &6/et check # &7to review these.";

				msg(p, text);
				return;
				
			}
			else if (open == 0 && closed == 0 && waiting == 0 && progress == 0)
				text = "&9[&FET&9]&F There are no tickets awaiting review at this time.";
			else
				text = "&9[&FET&9]&F There are tickets awaiting review:\n&2Open: "
						+ open
						+ " &6Waiting Response: "
						+ waiting
						+ " &cClosed: "
						+ closed
						+ "\n&FUse /et list and /et check # to review these.";

			msg(p, text);


	}

    @EventHandler
    public void onPlayerLogin(PlayerLoginEvent event)
    {
    	
    	String name = event.getPlayer().getName().toLowerCase();
   		int num=0;
			ResultSet rs = queryDB("SELECT id FROM " + dbprefix+"notes" + " WHERE  who='"+name+"'");

			if ( rs != null ) 
				try {
    				while(rs.next()) {
    					debug("notes found ...");
    					num++;
    				}
    				rs.close();
				}
				catch (SQLException e)
				{
					
				}
				
				if ( num > 0 )
				{
					for (Player p : getServer().getOnlinePlayers()) {
						if ( getLevel(p) >= 1 )
						{
							msg(p,"&9[&FET&9] &F"+event.getPlayer().getName()+" has notes in the database! Use &3/et notes "+event.getPlayer().getName()+ " view&F to see them!");
						}
					}
				}
					
				
				
				
    	if ( getConfig().getBoolean("main.loginReminder") == false )
    		return;
    	
    	final Player p = event.getPlayer();
   	
		plugin.getServer().getScheduler()
				.scheduleSyncDelayedTask(plugin, new Runnable() {
					public void run() {
						ticketReminder( p, false, false );
					}
				}, getConfig().getLong("main.loginReminderDelay")*20);

		return;
    }
    
    @Override
    public boolean onCommand(CommandSender sender, Command cmd, String label, String[] args) {
    	if (cmd.getName().equalsIgnoreCase("ticket") || cmd.getName().equalsIgnoreCase("et") ) {
    	
    	ArrayList<String> a = new ArrayList<String>();
    		
    		if ( args.length <= 0 || args[0].equalsIgnoreCase("help") )
    		{
    			int rank = getLevel(sender);
    			msg(sender,"Easy Tickets, by raum266 - version "+plugin.getDescription().getVersion());
    			msg(sender,"&3open    &F- Create a new ticket for the staff");
    			msg(sender,"&6list    &F- view all tickets - options: 'all' 'closed' 'open'");
    			msg(sender,"&3reply   &F- Reply to a ticket you have opened");
    			msg(sender,"&6close   &F- Close a ticket");
    			msg(sender,"&3remove  &F- Delete a ticket");
    			msg(sender,"&6info    &F- Display the info of a specific ticket");
    			if ( rank <= 0 )
    				return true;
    			msg(sender,"&3take    &F- claim a ticket");
    			msg(sender,"&6tp      &F- teleport to the ticket location");
    			msg(sender,"&3assign  &F- assign a ticket to a specific level");
    			msg(sender,"&6unassign&F- unclaim a ticket");
    			msg(sender,"&3notes   &F- view and/or set notes on a player");
    			msg(sender,"&6search  &F- search for a ticket by or responded by (player)");
    			return true;
    		}
    		

    		
    		
    		int num = 1;
    		while ( num < args.length )
    		{
    			a.add( args[num]);
    			num++;
    		}
    	
    		switch ( args[0].toLowerCase() )
    		{
    		case "new":
    		case "open": openTicket( sender, a); break;
    		case "l":
    		case "list": listTickets( sender, a ); break;
    		case "del":
    		case "delete": 
    		case "remove": removeTicket( sender, a ); break;
    		case "claim": 
    		case "take": takeTicket( sender, a ); break;
    		case "close": closeTicket( sender, a ); break;
    		case "reply": replyTicket(sender, a); break;
    		case "unassign": unassignTicket(sender,a); break;
    		case "assign": assignTicket( sender, a ); break;
    		case "tp": tpTicket(sender,a); break;
    		case "transfer": transferTicket(sender,a); break;
    		case "check":
    		case "view":
    		case "info": checkTicket(sender,a); break;
    		case "remind":  ticketReminder( (Player) sender, false, false ); break;
    		case "status": setStatus(sender, a); break;
    		case "purge": purgeTickets(sender, a); break;
    		case "notes": checkNotes(sender,a); break;
    		case "search": searchTickets(sender,a); break;
    		default: msg(sender,"&FThat is not a valid ticket command. Use &9/ticket help&F for support."); break;
    		}
    		
    	}
    	return true;
    }
    
    void searchTickets( CommandSender s, ArrayList<String> a)
    {
    	
    }
    
    public void checkNotes( CommandSender s, ArrayList<String> a)
    {
    	
    	if ( getLevel(s) <= 0 )
    	{
    		msg(s,"You do not have permission to check notes.");
    		return;
    	}
    	
    	if ( a.size() <= 1 )
    	{
    		msg(s,"Syntax: /et notes (player) (view/add/delete)");
    		return;
    	}

    	Player p = null;
    	String name = null;
    	
    	p = getServer().getPlayer(a.get(0) );
    	if ( p == null )
    		name = a.get(0).toLowerCase();
    	else
        	name = p.getName().toLowerCase();
    	
    	a.remove(0);
    	
    	if ( a.get(0).toLowerCase().startsWith("v")) {
    		String creator,note,who;
    		boolean found = false;
    		Long created;
    		int num=0;
    			ResultSet rs = queryDB("SELECT * FROM " + dbprefix+"notes" + " WHERE  who='"+name+"'");
    			if ( p != null ) name = p.getName();
    			
    			msg(s,"Displaying notes for "+ name+":");
    			
    			try {
    				while(rs.next()) {
    				creator = rs.getString("creator");
    				note = rs.getString("note");
    				created = rs.getLong("created");
    				who = rs.getString("who");
    				num++;
    				found = true;
    				msg(s,num+") " + note);
    				msg(s,"    "+creator+" , " + stamp(created));
    				}
    				rs.close();

    			} catch (SQLException e) {
    				e.printStackTrace();
    				error("View Notes," + name);
    				
    			}
    			if ( found == false )
    				msg(s,"No notes found.");
    	}
    	else if ( a.get(0).toLowerCase().startsWith("a")) {
    		a.remove(0); // remove 
    		String rep = "";
			while ( a.size() >= 1 ) {
	    		String line = a.get(0).replace("'", "`");
	    		rep = rep.trim() + " " + line;
	    		a.remove(0);
	    	}

			String query = "INSERT INTO " + dbprefix+"notes" + " (creator,note,created,who) VALUES('"+s.getName()+"',?, "+System.currentTimeMillis()+", ?)";
			writeDBPrep(query,rep,name);
			msg(s,"Note for player added! A reminder for the note will pop up the next time they log on!");
			return;

    	}
    	else     	if ( a.get(0).toLowerCase().startsWith("d")) {
    		a.remove(0);
    		boolean found = false;
    		Integer num=0;
    		int id = 0;
    			ResultSet rs = queryDB("SELECT id FROM " + dbprefix+"notes" + " WHERE  who='"+name+"'");
    			if ( p != null ) name = p.getName();

			try {
				while (rs.next()) {
					num++;
					if (num.toString().equals(a.get(0))) {
						found = true;
						break;
					}
				}
				rs.close();

			} catch (SQLException e) {
				e.printStackTrace();
				error("View Notes/Delete," + name);

			}
    			if ( found == false )
    			{
    				msg(s,"That note was not found.");
    				return;
    			}
    			
    			writeDB("DELETE FROM "+dbprefix+"notes WHERE id="+id);
    			msg(s,"Note deleted!");
    			return;
    	}
    	else
    	{
    		msg(s,"Valid options are view, add, delete");
    		return;
    	};
    	
    	
    }
    
    public void setStatus( CommandSender s, ArrayList<String> a)
    {
    	int ticket = getTicket(a);
    
		if ( a.size() <= 1 )
		{
			msg(s,"Valid statuses are OPEN, CLOSED, and WAITING");
			return;
		}

		if (!validTicket(ticket, s)) {
			msg(s, "That is an invalid ticket.");
			return;
		}
		
		String status = a.get(1).toString();
		int set = 0;
		switch (status.toLowerCase().charAt(0))
		{
		default: msg(s,"Valid statuses are OPEN, CLOSED, and WAITING"); return;
		case 'o': set = 1; break;
		case 'c': set = 5; break;
		case 'w': set = 4; break;
		}
		
		writeDB("UPDATE " + dbname + " SET status="+set+" WHERE id="+ticket);
		msg(s,"Status of ticket "+ticket+" changed to "+getStatus(set));
		broadcastTicket(s,ticket, "The status of ticket #"+ticket+" has been set to "+getStatus(set), false);
    	
    }

	public void broadcastTicket(CommandSender s, int ticket, String msg,
			Boolean all) {

		String assigned = "";
		String who = "";
		int level = 1;
		int rank = 0;

		if (ticket != -1) {
			ResultSet rs = queryDB("SELECT * FROM " + dbname + " WHERE  id="
					+ ticket);
			try {
				rs.next();
				level = rs.getInt("level");
				assigned = rs.getString("assigned");
				who = rs.getString("who");
				rs.close();
			} catch (SQLException e) {
				error("broadcastTicket: " + ticket);
				e.printStackTrace();
			}
		}
		
		for (Player p : getServer().getOnlinePlayers()) {

			if (all == true)
				rank = getLevel(p);

			if (rank >= level 
					|| s.getName().equalsIgnoreCase(p.getName()) 
					|| who.equalsIgnoreCase(p.getName())
					|| assigned.equalsIgnoreCase(p.getName()) )
				msg(p, msg);
		}

	}

	public void showTicket(CommandSender s, int ticket)
	{

		ResultSet rs = queryDB("SELECT * FROM " + dbname + " WHERE  id="
				+ ticket);
		int rank = getLevel(s);

		try {
			rs.next();
			int level = rs.getInt("level");

			if (s.getName().equalsIgnoreCase(rs.getString("who"))
					|| level <= rank) {
				msg(s, "&9Ticket #&F " + rs.getInt("id"));
				msg(s, "&9By: &F" + rs.getString("who"));
				msg(s, "&9Opened: &F" + stamp(rs.getLong("created")));
				msg(s, "&9Status: &F" + getStatus(rs.getInt("status")));
				msg(s, "&9Assigned To: &F" + rs.getString("assigned"));
				msg(s, "&9Level: &F" + rs.getInt("level"));
				msg(s, "&9Ticket:&F");
				msg(s, rs.getString("ticket").replace("`", "'"));
				if (rs.getString("staffreply").length() > 1)
					msg(s, "&9Staff Reply: &F" + rs.getString("staffreply").replace("`", "'"));
				if (rs.getString("userreply").length() > 1)
					msg(s, "&9Messages: &F" + rs.getString("userreply").replace("`", "'"));
				
				if ( rs.getString("who").equalsIgnoreCase(s.getName()))
					msg(s,"\n&FYou may use &9/et reply "+ ticket +" &Fto provide more information, or &9/et remove "+ticket+" &Fto remove this from the system.");
				else if ( rs.getString("assigned").length() <  2 )
					msg(s,"\n&FYou may use &9/et claim "+ ticket +" &F to claim this ticket.");
				else
					msg(s,"\n&FYou may use &9/et reply "+ ticket +" &F, or &9/et close "+ticket+" &Fto set the status as completed.");


			} else {
				msg(s, "You do not have permission to view that ticket.");
			}
			rs.close();
		} catch (SQLException e) {
			error("checkTicket : SQL Error");
		}
	}
	
	public void checkTicket(CommandSender s, ArrayList<String> a) {

		int ticket = getTicket(a);
		if (!validTicket(ticket, s)) {
			msg(s, "That is an invalid ticket.");
			return;
		}
		
		showTicket(s,ticket);
		
	}

	public String getStatus(int status) {
		switch (status) {
		default:return "&cUNKNOWN";
		case 1: return "&2OPEN";
		case 2: return "&9STAFF REPLIED";
		case 3: return "&5USER REPLIED";
		case 4: return "&6WAITING";
		case 5: return "&4CLOSED";
		case 6: return "&6CLAIMED";
		}
	}

    public String stamp ( long a )
    {
        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");

        Date resultdate = new Date(a);
        return sdf.format(resultdate);
        
    }
    public void transferTicket( CommandSender s,  ArrayList<String> a)
    {
    
    }
    
    public void listTickets( CommandSender s, ArrayList<String> a )
    {
    	
	int rank = getLevel(s);
	String query = "";
	String type = "";
		if (a.size() >= 1) {

			switch (a.get(0).toLowerCase().charAt(0)) {
			default:
				type = "OPEN";
				query = "SELECT * FROM " + dbname + " WHERE status = 1 OR status = 6 OR status = 3 OR status = 4";
				break;
			case 'a':
				type = "ALL";
				query = "SELECT * FROM " + dbname;
				break;
			case 'o':
				type = "OPEN";
				query = "SELECT * FROM " + dbname + " WHERE status = 1 OR status = 6 OR status = 3 OR status = 4";
				break;
			case 'c':
				type = "CLOSED";
				query = "SELECT * FROM " + dbname + " WHERE status = 5";
				break;
			}
		} else
			query = "SELECT * FROM " + dbname + " WHERE status != 5";

		msg(s,"&9[&FET&9]&F Listing "+type+" tickets:");
		int amt = 0;
		ResultSet rs = queryDB(query);
    	try {
    		
			while(rs.next()) {
			
				int level = rs.getInt("level");
				String ts = "&7" + getConfig().getString("tickets.listSyntax");
				if ( s.getName().equalsIgnoreCase(rs.getString("who")) || level <= rank ) 
				{
					String tick = rs.getString("ticket").replaceAll("`", "'");
					
					if ( tick.length() > 50 )
						tick = tick.substring(1,50) + "...";
					
					String assigned = rs.getString("assigned");
					if ( assigned.length() > 2 )
						assigned = " ("+assigned+")";
					
					ts = ts.replaceAll("%num%", ""+rs.getInt("id"));
					ts = ts.replaceAll("%status%", ""+getStatus(rs.getInt("status")));
					ts = ts.replaceAll("%level%", ""+level);
					ts = ts.replaceAll("%claimed%", assigned);
					ts = ts.replaceAll("%text%", tick);
					ts = ts.replaceAll("%by%", rs.getString("who"));
					msg(s, ts);
					amt++;
				}
				else { debug("Not your ticket"); }
			}
			rs.close();
		} catch (SQLException e) {
			error("listTickets : SQL Error");
		}
    	
    	if ( amt == 0 )
    		msg(s,"No tickets found.");
    	else
    		msg(s,"Type /et info # to view the status of a ticket.");
    	
    }
    public int getLevel( CommandSender s )
    {
    	int a = getConfig().getInt("main.maxLevel");
    	
    	if ( !(s instanceof Player) )
    	{
    		//debug("Console - level 100");
    		return a;
    	}
    	
    	while ( a > 0 )
    	{
    		if ( s.hasPermission("et.level."+a) )
    		{
    			//debug(s.getName() + " level " + a);
    			return a;
    		}
    		a--;
    	}
    	//debug("No level found");
    	return -1;
    }
    
    public void tpTicket( CommandSender s,  ArrayList<String> a)
    {
    	if ( !(s instanceof Player) )
    	{
    		msg(s,"Only a player can TP to a ticket location.");
    		return;
    	}
    	
    	int ticket = getTicket(a);
		if (!validTicket(ticket, s)) {
			msg(s, "That is an invalid ticket.");
			return;
		}
		
		tpToTicket(s,ticket);
    }

    void tpToTicket( CommandSender s, int ticket )
    {
    	ResultSet rs = queryDB("SELECT * FROM " + dbname + " WHERE  id="+ ticket);
		try {
			rs.next();
		Double x = rs.getDouble("x");
		Double y = rs.getDouble("y");
		Double z = rs.getDouble("z");
		World world = getServer().getWorld(rs.getString("world"));
		
		Player p = (Player) s;
		p.teleport(new Location(world, x, y, z) );
			
		} catch (SQLException e) {
			msg(s,"There was an error teleporting you to the ticket location!");
			e.printStackTrace();
		}
	
    }
    
    public void takeTicket( CommandSender s, ArrayList<String> a )
    {
    	int ticket = getTicket(a);
		if (!validTicket(ticket, s)) {
			msg(s, "That is an invalid ticket.");
			return;
		}
		
    	
     	if ( !(s instanceof Player) )
    	{
    		msg(s,"Only a player can take tickets, sorry console!");
    		return;
    	}
    	//Player p = (Player) s;
    	
		ResultSet rs = queryDB("SELECT * FROM " + dbname + " WHERE status != 5 AND id="	+ ticket);
		int rank = getLevel(s);

		try {
			rs.next();
			int level = rs.getInt("level");
			if ( level > rank) {
				rs.close();
				msg(s,"That ticket is unavailable for your ticket level.");
				return;
			}
			if ( rs.getString("assigned").length() > 1 ) 
			{
				msg(s,"That ticket has already been assigned. Teleporting you to ticket location...");
				tpToTicket(s, ticket );
				rs.close();
				return;
				
			}
			
			if ( getConfig().getBoolean("main.autoTeleport") == true )
				tpToTicket(s, ticket );
			broadcastTicket(s, ticket, "&9[&FET&9] &FTicket # "+ticket+" is currently under review by "+ s.getName(), true);
			showTicket(s,ticket);
			msg(s,"Use /et reply "+ticket+ " <msg> to send a response.");
			writeDB("UPDATE " + dbname + " SET assigned='"+s.getName()+"' WHERE id="+ticket);
			rs.close();
			} catch (SQLException e) {
				msg(s, "That is an invalid ticket.");
				//error("takeTicket : SQL Error");
			}
    }  
    
    public void closeTicket( CommandSender s, ArrayList<String> a )
    {
    	int ticket = getTicket(a);
    	
		if (!validTicket(ticket, s)) {
			msg(s, "That is an invalid ticket.");
			return;
		}
		
		writeDB("UPDATE " + dbname + " SET status=5 WHERE id="+ticket);
		writeDB("UPDATE " + dbname + " SET closed="+System.currentTimeMillis()+" WHERE id="+ticket);
		msg(s,"Status of ticket "+ticket+" changed to "+getStatus(5));
		broadcastTicket(s,ticket, "The status of ticket #"+ticket+" has been set to "+getStatus(5) + "\n&FYou may use /et check "+ticket+" to view the details.", true);
    }
    
    
    public void removeTicket( CommandSender s, ArrayList<String> a )
    {
    	int ticket = getTicket(a);
    	
		if (!validTicket(ticket, s)) {
			msg(s, "That is an invalid ticket.");
			return;
		}
		
		writeDB("DELETE FROM " + dbname + " WHERE id="+ticket);
		msg(s,"Ticket "+ticket+" deleted.");
		renumberTickets();
    }
    
    public void unassignTicket( CommandSender s, ArrayList<String> a )
    {
    	
    }
    
    public void assignTicket( CommandSender s, ArrayList<String> a )
    {
    	
    	int ticket = getTicket(a);
    	
		if (!validTicket(ticket, s)) {
			msg(s, "That is an invalid ticket.");
			return;
		}
		
		if (a.size() != 2)
		{
			msg(s,"Syntax: /et assign (ticket) (level)");
			return;
		}
		int num = -1;
    	try {
    	 num = Integer.parseInt(a.get(1));
     	} catch (NumberFormatException e) {
    		msg(s,"That is an invalid level.");
    		return;
    	}
    	
    	if ( num > getConfig().getInt("main.maxLevel") )
    	{
    		msg(s,"The max level for a ticket is "+ getConfig().getInt("main.maxLevel"));
    		return;
    	}

		
		writeDB("UPDATE " + dbname + " SET level="+num+ " WHERE id="+ticket);
		broadcastTicket(s,ticket, "&9[&FET&9] &FThe level of ticket "+ticket+" has been set to "+num+"\n&FYou may use &3/et check "+ticket+" &Fto view the details.", true);
		return;
	
    }
    
    public void replyTicket( CommandSender s, ArrayList<String> a )
    {
    	int ticket = getTicket(a);
    	
		if (!validTicket(ticket, s)) {
			msg(s, "That is an invalid ticket.");
			return;
		}
		
		ResultSet rs = queryDB("SELECT * FROM " + dbname + " WHERE id="	+ ticket);

		try {
			rs.next();
			if ( !rs.getString("who").equalsIgnoreCase(s.getName())
					&& !rs.getString("assigned").equalsIgnoreCase(s.getName())
					&& getConfig().getBoolean("main.allowUnassignedReply") == false )
			{
				rs.close();
				msg(s,"That is not your ticket to respond to. Type /et claim "+ticket+" to claim this ticket.");
				return;
			}
			
			if (rs.getString("who").equalsIgnoreCase(s.getName()))
			{
				broadcastTicket(s, ticket, "&9[&FET&9] &FTicket # "+ticket+" has been responded to by the writer.\nYou may use /et check "+ticket+ " to read this response.", true);
				String reply = rs.getString("userreply");
				String rep = reply + "\nUser: ";
				a.remove(0); // Remove Ticket Number from reply
				while ( a.size() >= 1 ) {
		    		String line = a.get(0).replace("'", "`");
		    		rep = rep.trim() + " " + line;
		    		a.remove(0);
		    	}
				
				 
				writeDB("UPDATE " + dbname + " SET status=3 WHERE id="+ticket);
				String query = "UPDATE " + dbname + " SET userreply=? WHERE id=?";
				writeDBPrep(query,rep,ticket);
				
			}
			else // Staff Response
			{
				broadcastTicket(s, ticket, "&9[&FET&9]  &FTicket # "+ticket+" has been responded to by the staff.\nYou may use /et check "+ticket+ " to read this response.", false);
				
				String reply = rs.getString("userreply");
				String rep = reply + "\nStaff: ";
				a.remove(0); // Remove Ticket Number from reply
				while ( a.size() >= 1 ) {
		    		String line = a.get(0).replace("'", "`");
		    		rep = rep.trim() + " " + line;
		    		a.remove(0);
		    	}
				

				writeDB("UPDATE " + dbname + " SET status=2 WHERE id="+ticket);
				//writeDB("UPDATE " + dbname + " SET staffreply='"+rep+"' WHERE id="+ticket);
				String query = "UPDATE " + dbname + " SET userreply=? WHERE id=?";
				writeDBPrep(query,rep,ticket);

			}
			
			rs.close();
			} catch (SQLException e) {
				
				error("replyTicket : SQL Error");
			}
		
    }
    
    public void openTicket( CommandSender s, ArrayList<String> a )
    {
    	
    	if ( a.size() < 1 )
    	{
    		msg(s,"Syntax: /et open (information)");
    		return;
    	}
    	
     	if ( !(s instanceof Player) )
    	{
    		msg(s,"Only a player can open a ticket - Sorry console.");
    		return;
    	}
     	
     	
    	int open = 0;
		ResultSet rs = queryDB("SELECT * FROM " + dbname + " WHERE status != 5 AND who='"+s.getName()+"'");
		try {
			
			while (rs.next()) {
				open += 1;
			}
			rs.close();
		} catch (SQLException e) {
			
		}

		if ( open >= getConfig().getInt("main.maxTickets"))
		{
			msg(s,"You have too many tickets already open. You must wait until some of your active tickets have been closed before creating any more.");
			return;
		}
    	
    Player p = (Player) s;
    	String who, world, assigned, staffreply, userreply, ticket = null;
    	int level, status;
    	long created, closed;
		double x, y, z;

    	who = "'" + p.getName() + "'";
    	created = System.currentTimeMillis();
    	world = "'" + p.getWorld().getName() + "'";
    	status = 1;
    	assigned = "''";
    	level = 1;
    	staffreply = "''";
    	userreply = "''";
    	x = p.getLocation().getX();
    	y = p.getLocation().getY();
    	z = p.getLocation().getZ();
    	closed = 0;

    	ticket = "'";
    	while ( a.size() >= 1 ) {
    		String line = a.get(0).replace("'", "`");
    		ticket = ticket.trim() + " " + line;
    		a.remove(0);
    	}
    	ticket = ticket + "'";
    	ticket = ticket.replace(";","");
    	
		broadcastTicket(s, -1, "&9[&FET&9]  &3"+who+"&F has opened a new ticket. Use /et list and /et check # to view this ticket.",true);
    	writeDB("INSERT INTO " + dbname + " (who, ticket, created, world, status, assigned, level, staffreply, userreply, x, y, z, closed) VALUES( "+who+","+ticket+","+created+","+world+","+status+","+assigned+","+level+","+staffreply+","+userreply+","+x+","+y+","+z+","+closed+");");
    	msg(s,"You have created a new ticket. The staff will respond as soon as possible!");
    	msg(s,"&FYou may use &9/et list to view all of your opened tickets, and &9/et check #&F to check on the progress.");
    	msg(s,"Remember: Tickets store location! If this is about a specific location, make sure you are AT that location when you submit!");
    }
    

    
    boolean validTicket( int ticket, CommandSender s )
    {
    	
    	if ( ticket == -1 ) return false;
    	String query = "SELECT level,who,assigned FROM "+ dbname + " WHERE id="+ticket;
    	ResultSet rs = queryDB(query);
		try {
			while (rs.next() )
			{
			int level = rs.getInt("level");
	    	int rank = getLevel(s);
	    	
	    	if ( rs.getString("who").equalsIgnoreCase(s.getName()) || rank >= level || rs.getString("assigned").equalsIgnoreCase(s.getName()))
	    	{
	    		rs.close();
	    		return true;
	    	}
			}
	    	rs.close();
	    		return false;
			
		} catch (SQLException e) {
			debug(""+query);
			e.printStackTrace();
			return false;
		}


    	}
    
    int getTicket( ArrayList<String> a )
    {
    	if ( a.size() < 1 ) return -1;
    	int num = -1;
    	try {
    	 num = Integer.parseInt(a.get(0));
     	} catch (NumberFormatException e) {
    		return -1;
    	}
    	
    	return num;
    }
    
    public boolean perm(CommandSender sender, String perm )
    {
    	if ( !(sender instanceof Player) )
    		return true;
    	
    	if ( sender.hasPermission("et.admin") ) return true;
    	if ( sender.hasPermission("et.*") ) return true;
    	
    	return ( sender.hasPermission("et." +perm) );
    			
    	
    }
    
    public void OpenDatabase() 
    {
    	String url = "";

    	if (getConfig().getBoolean("database.useMySQL")) {
			String user = getConfig().getString("database.user");
			String pass = getConfig().getString("database.password");
			String host = getConfig().getString("database.host");
			String db = getConfig().getString("database.dbname");
			String port = getConfig().getString("database.port");
			if (port.equals("0"))
				url = "jdbc:mysql://" + host + "/" + db;
			else
				url = "jdbc:mysql://" + host + ":" + port + "/" + db;
			try {
				conn = DriverManager.getConnection(url, user, pass);
				getServer().getLogger().info("Easy Tickets loading with MySQL.");
			} catch (SQLException e) {
				error("Unable to open database with MySQL - Check your database information - Trying SQLite");
				debug("" + e.getStackTrace());
			}

		} else if (getConfig().getBoolean("database.useSQLite")) {
			String sDriverName = "org.sqlite.JDBC";
			try {
				Class.forName(sDriverName);
			} catch (ClassNotFoundException e1) {
				error("Unable to load SqlDrivers : SQLite or MySQL is required for this plugin to function.");
				getServer().getPluginManager().disablePlugin(this);
				return;
			}

			url = "jdbc:sqlite:" + new File( getDataFolder(), "easytickets.db");
			try {
				conn = DriverManager.getConnection(url);
				getServer().getLogger().info("Easy Tickets loading with SQLite.");
			} catch (SQLException e) {
				error("Unable to load SqlDrivers : SQLite or MySQL is required for this plugin to function.");
				getServer().getPluginManager().disablePlugin(this);
			}
		}

		else {
		
			error("Unable to load SQLite or MySQL - Disabling plugin.");
			getServer().getPluginManager().disablePlugin(this);
			return;
		}

		
//		if ( getConfig().getInt("configVersion") < 1 )
//		{
			writeDB("CREATE TABLE IF NOT EXISTS " + dbprefix
					+ "tickets ( id integer PRIMARY KEY, who text, ticket text, created long, world text, status int, assigned text, level int, staffreply text, userreply text, x double, y double, z double, closed long  ) ");

			if (getConfig().getBoolean("database.useMySQL")) {
				writeDB("ALTER TABLE " + dbprefix + "tickets  CHANGE  `id`  `id` DOUBLE NOT NULL AUTO_INCREMENT");
				writeDB("ALTER TABLE " + dbprefix + "tickets CHANGE `x` `x` DOUBLE");
				writeDB("ALTER TABLE " + dbprefix + "tickets CHANGE `y` `y` DOUBLE");
				writeDB("ALTER TABLE " + dbprefix + "tickets CHANGE `z` `z` DOUBLE");
			}

//			info("Creating intial tables");
			addCol("closed","long");
//			getConfig().set("configVersion", 1);
//		}
		
//		if ( getConfig().getInt("configVersion") < 2)
//		{
//			info("Config version 2: Adding notes tables...");
			writeDB("CREATE TABLE IF NOT EXISTS " + dbprefix
					+ "notes ( id integer PRIMARY KEY, who text, creator text, note text, created long) ");
			writeDB("ALTER TABLE " + dbprefix + "notes  CHANGE  `id`  `id` DOUBLE NOT NULL AUTO_INCREMENT");
			getConfig().set("configVersion",2);
//		}
//		if ( getConfig().getInt("configVersion") < 3 )
//		{
			if (getConfig().getBoolean("database.useMySQL")) {
			writeDB("ALTER TABLE " + dbprefix + "notes  CHANGE  `id`  `id` DOUBLE NOT NULL AUTO_INCREMENT");
//			}
//			getConfig().set("configVersion",3);

		}
		
//		saveConfig();
}

public void addCol( String col, String type )
{
//	if (getConfig().getBoolean("database.useMySQL")) {
//		writeDB("ALTER TABLE "+ dbname + " ADD COLUMN "+ col + " " + type + " IF NOT EXISTS");
//	}
//	else // SQLite is horrible for this. :/
//	{
	
		try {
			ResultSet rs = queryDB("SELECT "+ col+" FROM "+dbname);
			rs.next();
			rs.close();
		} catch (SQLException e) {
			// Column doesn't exist - add it.
			writeDB("ALTER TABLE "+ dbname + " ADD COLUMN "+ col + " " + type);
		}
		
//	}
		
}
public void writeDBPrep(String query, Object input, Object id)
{
	PreparedStatement update = null;
	try {
		if (conn == null)
			OpenDatabase();
		conn.setAutoCommit(false);
		update = conn.prepareStatement(query);

		if ( input instanceof String )
			update.setString(1,(String) input);
		
		else if ( input instanceof Integer )
			update.setInt(1,(int) input);

		if ( id instanceof String )
			update.setString(2,(String) id);
		else if ( id instanceof Integer )
			update.setInt(2,(int) id);

		update.setQueryTimeout(10);
		update.executeUpdate();
		conn.commit();
		conn.setAutoCommit(true);
		update.close();
		
	} catch (SQLException e) {
		error("writeDBPrep error");
		e.printStackTrace();
	}

}
public void writeDB(String query) {

	try {
		if (conn == null)
			OpenDatabase();

		Statement statement = conn.createStatement();

		statement.setQueryTimeout(10);
		statement.executeUpdate(query);
	} catch (SQLException e) {
		error("writeDB error");
		debug(query + "");
	}

	return;

}

public ResultSet queryDB( String query) {

	ResultSet rs; 

	try {
		if (conn == null) {
			debug("queryDB: Database not open");
			OpenDatabase();
		}

		Statement statement = conn.createStatement();
		statement.setQueryTimeout(30);
		rs = statement.executeQuery(query);

		return rs;
	} catch (SQLException e) {
		error("queryDB error");
		debug("" + query);
		return null;
	}

	/*
	 * while(rs.next()) { // read the result set
	 * System.out.println("name = " + rs.getString("name"));
	 * System.out.println("id = " + rs.getInt("id")); }
	 */

}

public void CloseDatabase() {
	try {
		if (conn != null)
			conn.close();
	} catch (SQLException e) {
		// connection close failed.
		error("closeDatabase() error.");
	}
}


}