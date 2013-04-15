Lumper: Scaling your Postgres

functionality
	make multiple partitioned(and replicated) postgres instances seem as one logically

overall architecture
	the driver is used both by app server to speak with Lumper and by Lumper itself to speak with pg instances
	
	Lumper parses the input SQL string to get its cmd type - insert, update, or select, and decides execution mode
according to that.


