This project includes all notes and work
relating to the Udemy course taught by
Frank Kane.


Each Object in the Scala folder is a 
part of some activities. They are
broken down by their relevance
and complicatedness (not a word)



##Key Points From Each Object:
###AverageFriendsByName.scala
1. parseline function ie working with a predicate function to map method that parses csv data
2. .map, .mapValues, .reduceByKey. Note with .mapValues only works with pair RDDS
3. pair RDDS, are just tuples
###MinTemperatures.scala
1. Filtering with .filter(), used mainly to eliminate rows of an RDD, an xform
2. SortBy method
3. Printing Tuples and accessing their elements ._1
4. String interpolation. Nuff said
###WordCount.scala
1. Create helper method to pass to filter method
2. Using args, sbt shell to run scripts from the terminal. **todo: pass files as args instead of strings
3. flatMap() with a .split()+regex to extract words from a txt file, and create either a much smaller RDD or larger RDD. note this is completely different than using .split()+regex with map, thats to get data from a single line of input
###PurchaseByCustomer.scala
1. .toInt & .toFloat on tuple Strings after parsing from csv file
2. .take() method for testing data (do not use collect, waste of space/time)
3. calling foreach on a set of data brought to driver program
###PopularMoviesNicer.scala
1. Broadcast variables. Learned we can send a function to cluster which can have a map of the data we will eventually want to use. Later in program we "Fold in" the movie names from the Broadcast variable into the RDD we've manipulated about.
2. Chaining parameter to the map method. Notice we can Split, pull out a specific value, and map it to a number in 1 string
3. Creating a scala Map. Notice the syntax, variable type declared 
4. this syntax for a map movieNames += (fields(0).toInt -> fields(1))

