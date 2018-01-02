package Main

import java.util

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.regression.{IsotonicRegression, LabeledPoint}
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest}
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._
/**
  * Created by Administrator on 2017/4/24.
  */
object launch {
  def main(args: Array[String]): Unit = {

    //spark环境变量,采用kryo序列化
    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)
    //userInfoRDD
    var userInfoRDD = sc.textFile("user_info_format1.csv")
    //userLogRDD
    var userLogRDD = sc.textFile("user_log_format1.csv")
    //trainRDD
    var trainRDD = sc.textFile("train_format1.csv")
    //testRDD
    var testRDD = sc.textFile("test_format1.csv")
    // num of file
    println(userInfoRDD.count())
    println(userLogRDD.count())
    println(trainRDD.count())
    println(testRDD.count())

    //(userid , merchant , lable)
    var id_merchant_lableRDD =
    trainRDD.filter(item => item.split(",").length ==3)
      .map{case train => ((train.split(",")(0),(train.split(",")(1),train.split(",")(2))))}

    //(userid , age , gender)
    var id_age_gengerRDD =userInfoRDD.filter(item => item.split(",").length ==3)
    .map{case userInfo => ((userInfo.split(",")(0),(userInfo.split(",")(1),userInfo.split(",")(2))))}

    //(userid , itemid , catid , sellerid , brandid , timestamp , actiontype)
    //对userlog进行采样，采用方法采用pisson分布,抽取其中4000条进行训练
    var userid_sellerid_itemid_actiontypeRDD =
    userLogRDD.filter(item => item.split(",").length ==7)
      .map{case userlog => (userlog.split(",")(0),userlog.split(",")(1),userlog.split(",")(2),
      userlog.split(",")(3),userlog.split(",")(4),userlog.split(",")(5),userlog.split(",")(6))}
      .map{case (userid , itemid , catid , sellerid , brandid , timestamp , actiontype) =>
        ((userid,sellerid),(itemid , actiontype))
       }.sample(true,0.0001,100);


    //construct feature
    //(userid , age , gender , merchant , lable)
    var user_merchant_age_gender_lableRDD =
    id_merchant_lableRDD.join(id_age_gengerRDD)
      .map{case(userid , ((merchant , lable),(age , gender))) => ((userid , merchant), (age , gender,lable))}


    //give some feature
    var lable_userid_merchant_itemid_age_gender_actiontypeRDD =
    user_merchant_age_gender_lableRDD.join(userid_sellerid_itemid_actiontypeRDD)
      .map{case ((userid, merchant),((itemid , actiontype , lable),(age , gender)))
      => (lable+","+userid+","+merchant+","+itemid+","+age+","+gender+","+actiontype)}


    //construct feature rdd
    val featureTrainRDD = lable_userid_merchant_itemid_age_gender_actiontypeRDD
        .filter(line =>line.split(",").length == 7 && !line.split(",")(0).equals("")&& !line.split(",")(1).equals("")&& !line.split(",")(2).equals("")&& !line.split(",")(3).equals("")&& !line.split(",")(4).equals("")&& !line.split(",")(5).equals("")&& !line.split(",")(6).equals(""))
      .map{line=>
        var part = line.split(",")
        println("-------------------"+part(0)+"-------------------------")

        LabeledPoint(part(0).toDouble, Vectors.dense(part.tail.map(x => x.toDouble).toArray))

      }

    //svm unmInteration
    val numIterations = 20

    //use SVMWITHSGD,训练模型，其中使用的优化方法为随机梯度下降(SGD)


    val splits = featureTrainRDD.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val model = SVMWithSGD.train(trainingData, numIterations)

    /*
    *  使用训练好的模拟进行分类预测
    *
    */

    //预测数据集标准化
    var testmapRDD = testRDD.map{case user_merchant => (user_merchant.split(",")(0) , user_merchant.split(",")(1))}

    //待预测的rdd
    var pridicitonRDD =
      testmapRDD.join(id_age_gengerRDD)
      .map{
        case (userid , (merchant , (age , gender)))
        =>
          ((userid , merchant) , (age , gender))
      }.join(userid_sellerid_itemid_actiontypeRDD)
      .map{
        case((userid , marchant),((age , gender) , (itemid , actiontype)))
          =>
          ("what,"+userid+","+marchant+","+itemid+","+age+","+gender+","+actiontype)
      }.map{
        case line =>
          var part = line.split(",")
          println("-------------------"+part(0)+"-------------------------")

          LabeledPoint(part(0).toDouble, Vectors.dense(part.tail.map(x => x.toDouble).toArray))
      }

    //val xRDD = model.predict(pridicitonRDD)




    val labelAndPred = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction) }
    val trainErr = labelAndPred.filter(r => r._1 != r._2).count.toDouble / featureTrainRDD.count()



    //随机森林
    // Load and parse the data file.

    // Split the data into training and test sets (30% held out for testing)


    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val rfmodel = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = rfmodel.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()


    //用线性回归的方法进行预测
    // Run training algorithm to build the model
    val model_l = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(trainingData)

    // Compute raw scores on the test set.
    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model_l.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision

    //决策树

    val modelde = DecisionTree.trainClassifier(trainingData,2,Map[Int,Int](),"gini",10,30)

    // Evaluate model on test instances and compute test error
    val labelAnd = testData.map { point =>
      val prediction = modelde.predict(point.features)
      (point.label, prediction)
    }
    val test = labelAnd.filter(r => r._1 != r._2).count().toDouble / testData.count()


    println("the model of decision tree Test Error = " + test)
    println("the model of logistic regression Error = " + (1.0 - precision))
    println("the model of svm with sgd Error = " + trainErr)
    println("the model of random forest Error = " + testErr)
    println("Learned classification forest model:\n" + rfmodel.toDebugString)
    println("Learned classification tree model:\n" + modelde.toDebugString)

    sc.stop()
  }
}
