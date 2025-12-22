package bronze.utils

object LoadMetadata {
  def startLoad(entityName: String, recordSource: String, loadDate: java.time.LocalDate): Long = 1L
  def completeLoad(loadId: Long, recordsExtracted: Long, recordsLoaded: Long): Unit = ()
  def failLoad(loadId: Long, message: String): Unit = ()
}
