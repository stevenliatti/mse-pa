/** Enum for data status.
  *
  * @author Steven Liatti
  * @version 0.1
  */

package ch.hepia

sealed trait Status
case object PRESENT extends Status
case object DELETED extends Status
case object NEVER_PRESENT extends Status
case object UNKNOWN extends Status
