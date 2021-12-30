
// query 4: find user pairs (A,B) that meet the following constraints:
// i) user A is male and user B is female
// ii) their Year_Of_Birth difference is less than year_diff
// iii) user A and B are not friends
// iv) user A and B are from the same hometown city
// The following is the schema for output pairs:
// [
//      [user_id1, user_id2],
//      [user_id1, user_id3],
//      [user_id4, user_id2],
//      ...
//  ]
// "user_id" is the field from the users collection that you should use.
// Do not use the "_id" field in the users collection.

function suggest_friends(year_diff, dbname) {
    db = db.getSiblingDB(dbname);
    var pairs = [];
    // TODO: implement suggest friends
    // Return an array of arrays.
    var each_pair = [];
    var all_users = db.users.find({gender:'male'});
    all_users.forEach(function(myDoc){
      db.users.find({gender:'female'}).forEach(function(myDoc1){
        if (Math.abs(myDoc.YOB - myDoc1.YOB) < year_diff
        && myDoc.friends.indexOf(myDoc1.user_id) == -1
        && myDoc1.friends.indexOf(myDoc.user_id) == -1
        && myDoc.hometown.city == myDoc1.hometown.city){
          each_pair.push(myDoc.user_id,myDoc1.user_id);
          pairs.push(each_pair);
          each_pair = [];
        }
      })
    })
    // user.forEach(function(myDoc){results.push(myDoc.user_id);});
    return pairs;
}
