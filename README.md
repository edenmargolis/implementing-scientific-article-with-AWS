## Implementing the article: "Comparing Measures of Semantic Similarity" with AWS &amp; JAVA

**Running the project:**

Run our myJar JAR file via Terminal: java -jar myjar.jar

**Design:**

Our program has four major steps:
Golden standard analysis.
Counting and filtering the best thousand words in the corpus.
Building vectors by the requested formulas.
Machine learning by WEKA open-source interface.

**Golden standard analysis:**

In this part, we filter the unique words from the file.
Then, we upload two files to s3 bucket.
The first one is the original file and the second one is the filtered file.

**Counting and filtering the best 1000 words in the corpus:**

This part is built from two map-reduce programs.
The first one runs over the corpus and counts the number of occurrences for each word (each
word is a combination of lexeme-feature).
The second program gets the output of the first program and filters the best thousand words
omitting the first hundred.
  
**Building vectors by the requested formulas:**

This part is built with two map-reduce programs.
The first build feature vectors for each word in the golden standard and the second build the
comparable vector for the pair in the golden standard.
  
**First map-reduce program:**

In the setup method of the mapper, we import two files from s3.
The unique golden standard file that stores in HashMap for searching time efficiency and the best
thousand words that stores in vector.
Then we run over the corpus again and filter the words that appear in the golden standard list.
For each word that appears in the golden standard we look for the connected word
(lexeme-feature). If the connected word appears in the vector, we send four elements to the
reducer.
  
Key: *, Value: word, occurrences.
  
This pair represents the total number of connected words to a particular word.

Key: F, Value: Feature, occurrences, word.
  
This pair represents the total number of occurrences for each feature relative to a particular word.

Key: L, Value: Lexeme, occurrences, word.
  
This pair represents the total number of occurrences for each lexeme relative to a particular word.

Key: Z, word, Value: index, Lexeme, feature, occurrences.
  
This pair represents a value that should be added to the word vector. The index is taken from the
place of the connected word in the vector.

In the reducer part, we run over the pairs. Due to the alphabet order of the keys, we assume that
the pair with the "Z" key, will be the last pairs to show in the reducer.
For each other pair, we store the values in hash maps.
Then, for each "Z" pair, we create four vectors with a size of a thousand.
We run over the values and for each one calculate the four formulas and then store the result in
the relevant vector in the index we got in the value part of the pair.
Then the reducer builds a file with a word as key and number of formula and array as value.
  
**Second map-reduce program:**

In the setup method of the mapper, we import the golden standard file that stores in HashMap
represent the pairs in the file and their indication (false/true).
Then we run over the vectors we build in the first map-reduce program.
For each vector, we are looking at the instances of the word in the golden standard file.
For each instance we send the pair to reducer:

Key: index, Value: true/false indication, 0/1, array.
  
This key represents the index of the word in the golden standard. With this implementation, we
ensure that two words from the same pair we will show together in the reducer.
0/1 in the value represents either the word is the key or the value in the pair.
For each pair, we run over the value and store two lists, one for the key word in the pair and the
second for the value word in the pair. In total, we have four vectors for each word. Also, we save
the indication of the pair (true/false).
After then we run over the lists and for each index (four at all) we calculate the six formulas and
store the result in the output array.
In the end, the reducer builds a file that includes the array as a key and the indication as a value.

**Machine learning by WEKA open-source interface:**

In this part, we first import from s3 the result file of the last map-reduce program and parse it into
arff file.
Then, we load the created arff file, create the instances and the classifier as the assignment
requires.
Finally, we evaluate the instances, apply the 10-fold cross-validation, and then we create the final
output file which includes the precision, the recall, and the F-measure.
