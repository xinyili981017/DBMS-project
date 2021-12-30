// Find the oldest friend for each user who has a friend.
// For simplicity, use only year of birth to determine age. If there is a tie, use the friend with the smallest user_id
// Return a javascript object : the keys should be user_ids and the value for each user_id is their oldest friend's user_id
// You may find query 2 and query 3 helpful. You can create selections if you want. Do not modify the users collection.
//
//You should return something like this:(order does not matter)
//{user1:oldestFriend1, user2:oldestFriend2, user3:oldestFriend3,...}

function oldest_friend(dbname){
    db = db.getSiblingDB(dbname);
    var results = {};

    // return an javascript object described above
    db.users.aggregate([
      {$project: {
          "_id": 0,
          "user_id" : 1,
          "friends" : 1
      }},
      {$unwind:"$friends"},
      {$out: "flat_users"}
    ]); //find all friend pairs

    var cur = db.flat_users.find();

    while (cur.hasNext()) {
        var f = cur.next();
        db.flat_users.insert({user_id: f.friends, friends: f.user_id});
    }


    db.flat_users.aggregate([
        {$project: {
                "_id": 1,
                "user_id" : 1,
                "friends": 1
            }},
        {$group: { _id: "$user_id", friends: { $push: "$friends" } } },
        {$out: "entire_friends"}
    ]);
    

    var smallest_year;
    var oldest;
    db.entire_friends.find().forEach(function(myDoc1){
      smallest_year = 1000000;
      oldest = -1;
      if(myDoc1.friends.length == 0){
        return true;
      }
      else {
        for (var i = 0; i < myDoc1.friends.length; i++) {
          db.users.find({user_id: myDoc1.friends[i]}, {YOB: 1, _id: 0}).forEach(function (myDoc3) {
            if (myDoc3.YOB == smallest_year) {
              oldest = Math.min(myDoc1.friends[i], oldest);
            }
            if (myDoc3.YOB < smallest_year) {
              smallest_year = myDoc3.YOB;
              oldest = myDoc1.friends[i];
            }
          })
          results[myDoc1._id] = oldest;
        }
      }
    })

    return results;
}


