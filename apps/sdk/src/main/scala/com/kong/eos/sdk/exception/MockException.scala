
package com.kong.eos.sdk.exception

import scala.util.control.NoStackTrace

/**
 * This class id used to create custom exceptions that will be mostly used in tests
 * with the particularity that it has not trace.
 */
class MockException extends NoStackTrace {

}
