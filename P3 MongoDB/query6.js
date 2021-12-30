// query6 : Find the average user friend count
//
// Return a decimal as the average user friend count of all users
// in the users document.
function find_average_friendcount(dbname){
  db = db.getSiblingDB(dbname)
  // TODO: return a decimal number of average friend count
  //cannot work?!
  // var total_users = db.users.count();
  // var friends_num = db.users.aggregate([
  //   { "$group": {
  //       "_id": null,
  //        "count": {
  //            "$sum": { "$size": "$friends" }
  //        }
  //     }}
  //  ]);
  // return total_users / friends_num.result.count
  // {"MOB":3,"hometown":{"country":"Middle Earth","city":"Bywater","state":"The Shire"},
  // "current":{"country":"Middle Earth","city":"Bywater","state":"The Shire"},
  // "gender":"male","user_id":585,"DOB":1,"last_name":"TAYLOR","first_name":"Gotai","YOB":944,
  // "friends":[597,599,612,629,652,654,697,722,742]}
  var num_friends = 0;
  var total_users = 0;
  var all_users = db.users.find();
  all_users.forEach(function(myDoc){
    num_friends = num_friends + myDoc.friends.length;
    total_users += 1;
  });
  return num_friends/total_users
}
