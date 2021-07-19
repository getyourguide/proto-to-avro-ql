import java.util.concurrent.TimeUnit

object TimingUtils {
  def formatInterval(interval: Long): String = {
    val hr = TimeUnit.MILLISECONDS.toHours(interval)
    val min = TimeUnit.MILLISECONDS.toMinutes(interval - TimeUnit.HOURS.toMillis(hr))
    val sec = TimeUnit.MILLISECONDS.toSeconds(interval - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES.toMillis(min))
    val ms = TimeUnit.MILLISECONDS.toMillis(
      interval - TimeUnit.HOURS.toMillis(hr) - TimeUnit.MINUTES
        .toMillis(min) - TimeUnit.SECONDS.toMillis(sec)
    )
    f"$hr%02dh $min%02dmin $sec%02ds $ms%03dms"
  }

  def time[T](text: String)(block: => T): T = {
    val start = System.currentTimeMillis
    val res = block
    val totalTime = System.currentTimeMillis - start
    println(s"${text} Elapsed time: ${formatInterval(totalTime)}")
    res
  }
}
