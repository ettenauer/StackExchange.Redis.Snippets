# StackExchange.Redis.Snippets
Redis can be used as in-memory data structure store, a database, cache, or message broker. The StackExchange.Redis libary is a very popular library in the .net ecosystem to intgrate Redis. This repository includes a collection of code snippets which are using the StackExchange.Redis library to solve some selected common problems.

## Get Started
- run redis via docker-compose locally by using `https://github.com/ettenauer/StackExchange.Redis.Snippets/blob/main/local-environment/docker-compose.yml`
- unit tests configured in solution require local redis instance
## Code Snippets
### Use lua-script to coordinate action by lock via lease time and fencing token accross multiple instances
- code can be found: https://github.com/ettenauer/StackExchange.Redis.Snippets/tree/main/source/StackExchange.Redis.Snippets/Lua/Coordination
### Use stream to implement competitive consumer to process messages accross multiple instances
- code can be found: https://github.com/ettenauer/StackExchange.Redis.Snippets/tree/main/source/StackExchange.Redis.Snippets/Stream/CompetitiveConsumer
### Use stream to implement isolated consumer to process messages accross multiple instances
- code can be found: https://github.com/ettenauer/StackExchange.Redis.Snippets/tree/main/source/StackExchange.Redis.Snippets/Stream/IsolatedConsumer
