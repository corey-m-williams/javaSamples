package com.cwilliams.commerce.store.controller;

//Base class with log/error/warn methods defined
import com.cwilliams.commerce.LoggingBaseClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class SnippetLoader extends LoggingBaseClass {

	private volatile ScheduledExecutorService executor = null;
	private static volatile SnippetLoader curInstance = null;
	
	//URL base path for javascript snippets
	private String snippetLoc;
	private Map<String, String> snippetURLs;
	private volatile Map<String, String> htmlSnippets;
	//Snippets are cached for this long in seconds
	private int snippetCacheTime = 600;
	

	private SnippetLoader(){
		//Snippets stored on another server
		snippetLoc = "http://localhost:8181/snippets";
		snippetURLs = new HashMap<String, String>();
		snippetURLs.put("head", snippetLoc + "htmlHeadSnippet.html");
		snippetURLs.put("body1", snippetLoc + "htmlBodySnippet1.html");
		snippetURLs.put("body2", snippetLoc + "htmlBodySnippet2.html");
		this.htmlSnippets = new HashMap<String, String>();
		this.loadSnippets();
	}

	public static SnippetLoader getInstance(){
		if(curInstance == null){
            synchronized (SnippetLoader.class) {
                if(curInstance == null){
                    curInstance = new SnippetLoader();
                }
            }
		}
		return SnippetLoader.curInstance;
	}

	/**
	 * Initializes the Scheduled Executor Service to load the snippets every snippetCacheTime seconds
	 */
	private synchronized void loadSnippets(){
		Runnable loader = new Runnable(){
			public void run(){
				String snippetText = "";
				Map<String, String> newSnippets = new HashMap<String, String>();
				try{
					for(String key : snippetURLs.keySet()){
						snippetText = "";
						//System.out.println("Loading " + key);
						URL snippetURL = new URL(snippetURLs.get(key));
						BufferedReader in = new BufferedReader(new InputStreamReader(snippetURL.openStream()));
						String snippetLine;
						while((snippetLine = in.readLine()) != null){
							snippetText += snippetLine + "\n";
							//System.out.println(snippetLine);
						}
						//System.out.println("Done loading " + key);
						newSnippets.put(key, snippetText);
					}
				}catch (IOException e){
					error("Error loading snippets files...", e);
					return;
				}
				htmlSnippets = newSnippets;

			}
		};
		//If the snippet loader is already running, don't start it again
		if(executor != null){
			return;
		}
		System.out.println("Starting new executor");
		executor = new ScheduledThreadPoolExecutor(1);
		executor.scheduleWithFixedDelay(loader, 0, snippetCacheTime, TimeUnit.SECONDS);
	}
	
	public String getSnippetLoc() {
		return snippetLoc;
	}

	public void setSnippetLoc(String snippetLoc) {
		this.snippetLoc = snippetLoc;
	}

	public Map<String, String> getHtmlSnippets() {
		return htmlSnippets;
	}

	public void setHtmlSnippets(Map<String, String> htmlSnippets) {
		this.htmlSnippets = htmlSnippets;
	}
}
