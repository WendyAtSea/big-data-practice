# Calculate Top N Terms in a File Using Hadoop MapReduce

Load a text file to Hadoop and use MapReduce to find the most frequently used
words and their count.

#### screenshots
![Screenshot]()

## Development Setup
Download the source code at
* [Version 1.0](https://github.com/WendyAtSea/big-data-practice/)

You need to install the following software:
* Apache Maven 3.+
* Java 1.8+
* Access to Hadoop stand-alone or cluster

To build:
```sh
cd WordCount
mvn clean install
```

## Usage

If you don't have a text file to analyze, you can download the following two files:
* Ulysses: http://www.gutenberg.org/files/4300/4300-0.txt  
* Stopwords: https://raw.githubusercontent.com/stanfordnlp/CoreNLP/master/data/edu/stanford/nlp/patterns/surface/stopwords.txt

Run the following command on Hadoop to calculate 100 most frequent words along with number of occurrence in Ulysses exclude the stop words in stopwords.txt. The result is saved in output
> hadoop fs -put stopwords.txt input/stopwords.txt 
> hadoop fs -put 4300-0.txt input/4300-0.txt
> hadoop fs -rm -r output
> hadoop jar target/wordcount-1.0-SNAPSHOT.jar com.bluerain.hadoop.mapred.WordCount -n 100 -s stopwords.txt 4300-0.txt output 

## Contributing

1. Fork it (<https://github.com/yourname/yourproject/fork>)
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request
