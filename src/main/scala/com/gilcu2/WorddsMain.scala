package com.gilcu2

object WorddsMain {

  val dictionary = Set(
    "a",
    "it",
    "leap",
    "able",
    "air",
    "hair",
    "pre",
    "table",
    "tab",
    "chair",
    "apple",
    "app",
    "cupboard",
    "cup",
    "board",
    "lept",
    "lepton",
    "ton",
    "on"
  )

  def splitInWords(s: String, dictionary: Set[String]): (Seq[String], String) = {

    if (s.isEmpty)
      return (Seq(), "")

    var last = 1

    while (!dictionary.contains(s.substring(0, last)) && last < s.length)
      last += 1

    if (last == s.length)
      return (Seq(), s)

    val word = s.substring(0, last)
    val (others, rest) = splitInWords(s.substring(last, s.length), dictionary)

    if (rest.isEmpty)
      (Seq(word) ++ others, "")
    else
      splitInWords(s, dictionary.filter(_ != word))

  }

  def splitInWords1(s: String, dictionary: Set[String], initial_size: Int): (Seq[String], String) = {

    if (s.isEmpty)
      return (Seq(), "")

    var last = initial_size
    var r = (Seq[String](), "")

    while (last < s.length) {
      last += 1

      val found = dictionary.contains(s.substring(0, last))

      if (found) {
        val word = s.substring(0, last)

        val (others, rest1) = splitInWords1(s.substring(last, s.length), dictionary, 1)
        r = (Seq(word) ++ others, rest1)

      }
      
    }
    r
  }

  def main(implicit args: Array[String]): Unit = {

    val s = "tableapplechairtablecupboard"

    val words = splitInWords1(s, dictionary, 1)

    println(s"The words of $s are: $words")


  }

}

