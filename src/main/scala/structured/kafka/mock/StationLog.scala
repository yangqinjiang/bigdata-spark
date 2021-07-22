package structured.kafka.mock
/**
 * 基站通话日志数据，字段如下：
 ** @param stationId 基站标识符ID
 * @param callOut 主叫号码
 * @param callIn 被叫号码
 * @param callStatus 通话状态
 * @param callTime 通话时间
 * @param duration 通话时长
 */
case class StationLog(stationId: String, //
                      callOut: String, //
                      callIn: String, //
                      callStatus: String, //
                      callTime: Long, //
                      duration: Long //
 ){
  override def toString: String = s"$stationId,$callOut,$callIn,$callStatus,$callTime,$duration"
}
