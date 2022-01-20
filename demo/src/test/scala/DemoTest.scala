import org.junit.jupiter.api.Test

class DemoTest {

  @Test
  def testDemo(): Unit = {
    System.setProperty("importPath", "docker/import")
    Demo.main(Array.empty)
  }

}
