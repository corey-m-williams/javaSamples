# Java Code Samples

## SnippetLoader

I had to get a new jsf application to have the same header/footer/menu as a Drupal site.
In order to get this working, I set up a drupal module that loaded a page into a DOM tree, and then I parsed through the tree taking out the body content, sorting and aggregating all of the javascript and CSS tags that could be aggregated, and storing the rest in individual files.

This Loader is what pulls those snippets from the Drupal server, and loads them into the JSF app, to where they can be used via EL expressions into the appropriate base page template.
It starts up an Executor that reloads the snippets every 10 minutes, building a new object while doing all of the network requests, then it atomically swaps (just a single volatile reference swap) the new snippets into place once loaded.


## AccessLoggerQueue

On an e-commerce project that deals with accessing documents for a particular Race Track/Date/Race Number combination, I had the task of updating the database upon every usage of an 'entitlement', or a view of a document.
The in-memory values will have already been updated when the document is viewed, this is just persisting the change to the DB.

I set this up as a Queue that takes a ProductView object, which has all of the information needed to log an access.
The queue processes chunks of up to 50 accesses at any one time.  If one of those chunks happens to fail, the DB transaction is cancelled, and the chunk is written out to file to be tried again once the queue is empty.
If, when processing the file, there is an error, the queue then sends an email out to an admin to let them know something has gone wrong multiple times.
