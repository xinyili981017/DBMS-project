// query 8: Find the average user friend count for all users with the same hometown city
// using MapReduce
// Using the same terminology in query7, we are asking you to write the mapper,
// reducer and finalizer to find the average friend count for each hometown city.


var city_average_friendcount_mapper = function() {
  // implement the Map function of average friend count
  emit(this.hometown.city,{total_users:1,num_friends:this.friends.length});
};

var city_average_friendcount_reducer = function(key, values) {
  // implement the reduce function of average friend count
  reducedVal = {total_users:0,num_friends: 0};
  for (var idx = 0; idx < values.length; idx++) {
       reducedVal.total_users += values[idx].total_users;
       reducedVal.num_friends += values[idx].num_friends;
   }
   return reducedVal;

};

var city_average_friendcount_finalizer = function(key, reduceVal) {
  // We've implemented a simple forwarding finalize function. This implementation
  // is naive: it just forwards the reduceVal to the output collection.
  // You may need to change it to pass the test case for this query
  var ret = reduceVal.num_friends/reduceVal.total_users;
  return ret;
}
