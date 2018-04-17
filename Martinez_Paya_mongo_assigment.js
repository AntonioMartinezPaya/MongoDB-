// Exercise 1
//Nombre en dos columnas:
db.coffee.find({"contract.country":"United Kingdom", "Client.Surname":{$size:2}},{"Client.Name":1,"Client.Surname":1,"_id":0,"contract.address":1});




db.coffee.aggregate([{$match:{"contract.country":"United Kingdom", "Client.Surname":{$size:2}}}, 
				{$project:{"_id": 0,"contract.address":1, 
							fullname:{$concat:["$Client.Name", " ", {$arrayElemAt: ["$Client.Surname", 0]}, " ", {$arrayElemAt: ["$Client.Surname", 1]}]}}}])

//Exercise 2
db.coffee.find({$or:[{$and:[{"Movies":{$exists:false}},{"Series":{$exists:false}}]}, 
	                 {$and:[{"Movies":{$exists:false}},{"Series":{$size:1}}]}, 
	                 {$and:[{"Movies":{$size:1}},{"Series":{$exists:false}}]}]},
	                 {"contract.contract ID":1 ,"billing":1,"_id":0});



//Exercise 3

db.coffee.find({"Client.Email":{ $regex: '.*@[^\.]+\.[^\.]+$', $options: 'imxs' }},{})

//Exercise 4
db.coffee.aggregate([{$unwind:"$Movies"} ,{$match:{"Movies.Viewing PCT":{$gt: 30, $lt:50}}}  , { $group:{_id:"$Movies.Details.Country", "ntaps":{$sum:1}}}])


//Exercise 5
db.coffee.aggregate([
	{$project:{"user": {
             "dni": "$Client.DNI", 
             "name": "$Client.Name", 
             "surname": "$Client.Surname",
             "total_amount":"$TOTAL",
         },         
         "taps":{$concatArrays:["$Movies","$Series"]}
     }},
    {$unwind:"$taps"},
    {$group: {_id:"$user","total_taps":{$sum:1},"total_traffic":{$sum:{$sum:[{$sum:{$multiply:["$taps.Details.Duration",{$divide:["$taps.Viewing PCT",100]}]}},{$sum:{$multiply:["$taps.Avg duration",{$divide:["$taps.Viewing PCT",100]}]}}]}}}}
])
//top 10 facebook likes


db.coffee.aggregate([{$project:{"Movies.Title":1,"Movies.Details.Facebook likes":1,"_id":0}},{$unwind:"$Movies"},{$group:{"_id":{"Titulo":"$Movies.Title","Likes":"$Movies.Details.Facebook likes"}}},{$sort:({"_id.Likes": -1})},{$limit:10}])

db.coffee.aggregate([{$unwind:"$Movies"},{$match:{"Movies.Details.Year":{$lt:2000}}},{$project:{"Movies.Title":1,"Movies.Details.Facebook likes":1,"_id":0}},{$group:{"_id":{"Titulo":"$Movies.Title","Likes":"$Movies.Details.Facebook likes"}}},{$sort:({"_id.Likes": -1})},{$limit:10}])

db.coffee.aggregate([{$unwind:"$Movies"},{$match:{"Movies.Details.Country":"Spain"}},{$project:{"Movies.Title":1,"Movies.Details.Facebook likes":1,"_id":0}},{$group:{"_id":{"Titulo":"$Movies.Title","Likes":"$Movies.Details.Facebook likes"}}},{$sort:({"_id.Likes": -1})},{$limit:10}])

