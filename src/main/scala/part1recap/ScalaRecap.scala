package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap {

  //values and variable
  val aBoolean: Boolean = false

  //expression
  val anIfExpression = if (2 > 3) "bigger" else "true"

  //instruction vs expression
  val theUnit = println("hello there") //unit -no meaningfull value or void

  //functions
  def myFunctions(x: Int) = 42

  //oop
  class Animal

  class Dog extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("crunch crunch")
  }

  //singleton pattern
  object MySingleton

  //companion
  object Carnivore

  //generics
  trait MyList[A]

  //method notation

  val x = 1 + 2
  val y = 1.+(2)

  //functional programming
  val incrementer: Int => Int = x => x + 1
  val incremented = incrementer(42)
  //map,flatmap,filter
  val processedList = List(1, 2, 3).map(incrementer)

  //pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "not available"
  }
  //try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "nahi beta"
    case _ => "Something else"
  }

  //Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture=Future{
  //some expensive computation,runs on another thread
  42
  }
  aFuture.onComplete{
    case Success(meaningOfLife) => println(s"I've found $meaningOfLife")
    case Failure(exception) =>println(s"i've failed $exception")
  }
  //partial function
  val aPartialFunction:PartialFunction[Int,Int]={
    case 1 =>43
    case 8 =>56
    case _ =>999
  }
  //Implicits
  //auto-injection by compiler
  def methodWithImplicitArguments(implicit x:Int)=x+43
  implicit val implicitsVal=67

  methodWithImplicitArguments

    //implicit conversion
  case class Person(name:String){
      def greet=println(s"hi my name is $name")
    }
  implicit def fromStringToPerson(name:String)=Person(name)
  "Bob".greet  //fromStringToPerson("Bob).greet

  //implicit conversion by implicit class
 implicit class Doggy(name:String){
    def bark=println("Bark")
  }
  "Lassie".bark
   /*
   local scope
   imported scope
   companion object of the types involves in the method call
    */
  List(1,2,3).sorted
}






