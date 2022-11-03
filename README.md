# loadtester-go

---

If you want to send http requests with a specific HTTP Method and not validate anything as well as you don't care about connection reuse, then use [ab](https://httpd.apache.org/docs/2.4/programs/ab.html) instead.

Use this "framework" when you need to validate http responses in non-trivial ways, the kind of load you need to generate is not a simple http request, the kind of load you need to generate needs to increase over time in a non-trivial fashion, you desire to spread the load as evenly over your sampling interval as possible, or you need to measure exactly what's going on with your load generator host concurrent workers vs tasks for some time interval.

There is a trivial implementation present in [example](./loadtester/example/main.go), but know that if you are wanting to create a http request generating task you should use a single http client using the transport returned from NewHttpTransport() to avoid creating too many short-lived connections. Also on context error/shutdown make sure your http clients' CloseIdleConnections() method gets called. Also, don't forget to always read all of the response body of the http response to ensure you achieve connection reuse! You can find a [stateless http loadtest example here](./loadtester/example_http/main.go)
 
Features:

1. Worker goroutines should never break, even if the tasks they perform panic.
1. can retry tasks by the task implementing the DoRetryer or DoRetryChecker interface
1. Can attempt to flush retries up to some timeout duration via the config option FlushRetriesOnShutdown(true) and FlushRetriesTimeout(<time.Duration>) if you have the need to try and get every task to pass before a normal shutdown event.
1. Bring your own validation routines as part of your task definitions or Data Driven Tests
1. Implement your own logic for adjusting the load generation interval, the concurrent number of workers, and the number of tasks to complete each interval over time any way you see fit via the ConfigUpdate struct and the UpdateConfigChan any way you choose.
1. Send any kind of load using any kind of client you can think of. You could even implement an entire workflow as a task if you like.
1. Use the parent context to stop a loadtest after some amount of time if you like.
1. Bring your own logger, just needs to satisfy the SugaredLogger interface.
1. Bound your loadtest to a specific count of tasks via the MaxTasks() option or by returning less tasks than ReadTasks prompts you to populate.
1. Gracefully flushes all data and tasks when the context is canceled.
1. Prevents overrunning the upstream with extreme bursts if it experiences some kind of saturation and latency increases creating back pressure in the loadtester. Such issues are clearly present in the output metrics as lag for the current loadtest config/plan.
1. This is trivial to control via some thin control plane wrapper and coordinate multiple nodes and configs together.
1. All load spaced greater than 20 milliseconds apart is evenly created. ( 20ms was chosen due to that being the average time for GC to interrupt processes ) All load that would be spaced less than 20 milliseconds apart is created as quickly as possible but each interval is still spaced before the next batch of load is created.