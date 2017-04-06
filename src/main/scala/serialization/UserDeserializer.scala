package serialization

import java.io.{ByteArrayInputStream, ObjectInputStream, StreamCorruptedException}
import java.util

import models.User
import org.apache.kafka.common.serialization.Deserializer

class UserDeserializer extends Deserializer[User]{

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

  }

  override def deserialize(topic: String, data: Array[Byte]): User = {

    try{
      val byteIn = new ByteArrayInputStream(data)
      val objIn = new ObjectInputStream(byteIn)
      val obj = objIn.readObject.asInstanceOf[User]
      byteIn.close()
      objIn.close()
      obj
    }catch {
      case ex: StreamCorruptedException => throw ex
      case ex: Exception => throw new Exception(ex.getMessage)
    }

  }

  override def close(): Unit = {

  }

}
