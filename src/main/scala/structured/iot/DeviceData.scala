package structured.iot
/**
 * 物联网设备发送状态数据
 * @param device 设备标识符ID
 * @param deviceType 设备类型，如服务器mysql, redis, kafka或路由器route
 * @param signal 设备信号
 * @param time 发送数据时间
 */
case class DeviceData(
                       device: String, //
                       deviceType: String, //
                       signal: Double, //
                       time: Long
                     )
