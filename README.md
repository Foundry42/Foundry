*This is the Foundry Dev Repo README*

Foundry is infrastructure for compute sharing and utilization. Suppliers can download Foundry packages and run them with nohang to share cycles. Consumers can request foundry nodes, ssh into them, and use them as ec2-style compute workers.

Undergirding Foundry is a set of state-of-the-art fault-tolerant and workload aware schedulers that help maximize utilization and liquidity in the network. Foundry is also heavily integrated with leading tools for distributed computing, such as Ray and Dask Distributed.

Process next steps
- [ ] create trello dashboard to track and  organize eng progress

Next steps
- [ ] seed npm package template on user side
- [ ] create initial coordination database (e.g. to hold ip addresses)
- [ ] create foundry init command to create a worker ip and procure a worker node on foundry.stanford.edu


Next steps:
- [ ] cluster capacity analysis scripts dev
- [ ] create a json config doc that you can feed as input to the cluster capacity scripts which reveal capacity for the specific partitions that you have been granted access to.
- [ ] find a way to download historical data on usage patterns from Slurmdb
- [ ] watch all database units from Angela and decide on db architecture; MongoDB vs. sql vs. Airtable.
- [ ] start to architect user side of the picture
- [ ] downlaod binhang's code and try to figure out how you might run it on the Stanford side.
- [ ] start writing the whitepaper
- [ ]...  


***Architectural notes***

Initial focus -> sinfo config files and scripts for getting data on available compute. 

Do not over automate. The cluster config can be manually generated for a while. Much later on, if necessary, you can create a script. However, the naming differs across slurm clusters so somewhat easier just to manually create a JSON config.
