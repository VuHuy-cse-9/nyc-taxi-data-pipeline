// Initiate replica set with single member
rs.initiate({
  _id: "rs0",
  members: [
    {
      _id: 0,
      host: "datasource3:27017"  // Use service name if connecting from other containers
    }
  ]
});