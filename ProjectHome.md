Use sflow.js to implement logging using sFlow (http://www.sflow.org). The purpose is for continuous, real-time monitoring of large web clusters. The sFlow mechanism allows for a random 1-in-N sample of the URL transactions to be reported, along with a periodic snapshot of the most important counters, all using sFlow's efficient XDR-encoded UDP "push" model. There is no limit to the number of web-servers that can be sending to a single sFlow collector.

sFlow monitoring in node.js is designed to work together with sFlow monitoring in switches, routers and servers. For details and examples, see:

http://blog.sflow.com/2011/08/nodejs.html