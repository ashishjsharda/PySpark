from pyspark import SparkContext
if __name__=="__main__":
    sc=SparkContext("local[3]","word count")
    sc.setLogLevel("ERROR")
    lines=sc.textFile("output_city.txt")
    words=lines.flatMap(lambda line:line.split(" "))
    wordCount=words.countByValue()
    for word,count in wordCount.items():
        print("{ } : { }".format(word,count))
