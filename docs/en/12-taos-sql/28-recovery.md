---
sidebar_label: Error Recovery
title: Error Recovery
---

In a complex environment, connections and query tasks may encounter errors or fail to return in a reasonable time. If this occurs, you can terminate the connection or task.

## Terminate a Connection

```sql
KILL CONNECTION conn_id;
```

You can use the SHOW CONNECTIONS statement to find the conn_id.

## Terminate a Query

```sql
SHOW QUERY query_id;
```

You can use the SHOW QUERIES statement to find the query_id.

## Terminate a Transaction

```sql
KILL TRANSACTION trans_id
```

You can use the SHOW TRANSACTIONS statement to find the trans_id.

## Reset Client Cache

```sql
RESET QUERY CACHE;
```

If metadata becomes desynchronized among multiple clients, you can use this command to clear the client-side cache. Clients then obtain the latest metadata from the server.
