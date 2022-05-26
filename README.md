*This is the Foundry Dev Repo README*

Foundry is infrastructure for compute sharing and utilization. Suppliers can download Foundry packages and run them with nohang to share cycles. Consumers can request foundry nodes, ssh into them, and use them as ec2-style compute workers.

Undergirding Foundry is a set of state-of-the-art fault-tolerant and workload aware schedulers that help maximize utilization and liquidity in the network. Foundry is also heavily integrated with leading tools for distributed computing, such as Ray and Dask Distributed.

Process next steps
- [ ] create trello dashboard to track and  organize eng progress

Next steps
- [ ] seed npm package template on user side
- [ ] create initial coordination database (e.g. to hold ip addresses)
- [ ] create foundry init command to create a worker ip and procure a worker node on foundry.stanford.edu
