
package com.kong.eos.plugin.cube.field.hierarchy

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpecLike}

@RunWith(classOf[JUnitRunner])
class HierarchyFieldTest extends WordSpecLike
with Matchers
with BeforeAndAfter
with BeforeAndAfterAll
with TableDrivenPropertyChecks {

  var hbs: Option[HierarchyField] = _

  before {
    hbs = Some(new HierarchyField())
  }

  after {
    hbs = None
  }

  "A HierarchyDimension" should {
    "In default implementation, get 4 precisions for all precision sizes" in {
      val precisionLeftToRight = hbs.get.precisionValue(HierarchyField.LeftToRightName, "")
      val precisionRightToLeft = hbs.get.precisionValue(HierarchyField.RightToLeftName, "")
      val precisionLeftToRightWithWildCard = hbs.get.precisionValue(HierarchyField.LeftToRightWithWildCardName, "")
      val precisionRightToLeftWithWildCard = hbs.get.precisionValue(HierarchyField.RightToLeftWithWildCardName, "")

      precisionLeftToRight._1.id should be(HierarchyField.LeftToRightName)
      precisionRightToLeft._1.id should be(HierarchyField.RightToLeftName)
      precisionLeftToRightWithWildCard._1.id should be(HierarchyField.LeftToRightWithWildCardName)
      precisionRightToLeftWithWildCard._1.id should be(HierarchyField.RightToLeftWithWildCardName)
    }

    "In default implementation, every proposed combination should be ok" in {
      val data = Table(
        ("i", "o"),
        ("google.com", Seq("google.com", "*.com", "*"))
      )

      forAll(data) { (i: String, o: Seq[String]) =>
        val result = hbs.get.precisionValue(HierarchyField.LeftToRightWithWildCardName, i)
        assertResult(o)(result._2)
      }
    }
    "In reverse implementation, every proposed combination should be ok" in {
      hbs = Some(new HierarchyField())
      val data = Table(
        ("i", "o"),
        ("com.stratio.sparta", Seq("com.stratio.sparta", "com.stratio.*", "com.*", "*"))
      )

      forAll(data) { (i: String, o: Seq[String]) =>
        val result = hbs.get.precisionValue(HierarchyField.RightToLeftWithWildCardName, i.asInstanceOf[Any])
        assertResult(o)(result._2)
      }
    }
    "In reverse implementation without wildcards, every proposed combination should be ok" in {
      hbs = Some(new HierarchyField())
      val data = Table(
        ("i", "o"),
        ("com.stratio.sparta", Seq("com.stratio.sparta", "com.stratio", "com", "*"))
      )

      forAll(data) { (i: String, o: Seq[String]) =>
        val result = hbs.get.precisionValue(HierarchyField.RightToLeftName, i.asInstanceOf[Any])
        assertResult(o)(result._2)
      }
    }
    "In non-reverse implementation without wildcards, every proposed combination should be ok" in {
      hbs = Some(new HierarchyField())
      val data = Table(
        ("i", "o"),
        ("google.com", Seq("google.com", "com", "*"))
      )

      forAll(data) { (i: String, o: Seq[String]) =>
        val result = hbs.get.precisionValue(HierarchyField.LeftToRightName, i.asInstanceOf[Any])
        assertResult(o)(result._2)
      }
    }
  }
}
