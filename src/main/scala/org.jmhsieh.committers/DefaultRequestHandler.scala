package org.jmhsieh.committers

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.hadoop.hbase.client.Table
import org.mortbay.jetty.{HttpConnection, Request}
import org.mortbay.jetty.handler.AbstractHandler
import org.slf4j.LoggerFactory

/**
  * Created by skinibizapps on 11/25/16.
  */
class DefaultRequestHandler(hBaseConnectionFactory: HBaseConnectionFactory) extends AbstractHandler {
  val log = LoggerFactory.getLogger("DefaultRequestHandler")
  //val hBaseCommitterFactory = HBaseCommitterFactory
  log.info("Hello World from Scala")

  val table = hBaseConnectionFactory.getTable(hBaseConnectionFactory.DEFAULT_NAME)

  override def handle(s: String, httpServletRequest: HttpServletRequest, httpServletResponse: HttpServletResponse, i: Int) : Unit = {
    log.info("Calling the handle method of the RequestHandler object")
    log.info(s"String value is $s")
    val baseRequest =
      if (httpServletRequest.isInstanceOf[Request])
        httpServletRequest.asInstanceOf[Request]
      else
        HttpConnection.getCurrentConnection.getRequest
    baseRequest.setHandled(true)
    httpServletResponse.setContentType("text/html;charset=utf-8")
    httpServletResponse.setStatus(HttpServletResponse.SC_OK)
    httpServletResponse.getWriter.println("<h1>Committers</h1>")
    httpServletResponse.getWriter.println("<table><tr><th>Name</th><th>Hair</th><th>Beard</th></tr>")
    if (hBaseConnectionFactory == null) {
      httpServletResponse.getWriter.println("</table>")
      httpServletResponse.getWriter.println("No connection to hbase.  Try again?")
         return
    }

    val it = hBaseConnectionFactory.scanner(null, null, table)
    var i = 0
    for (c <- it) {
      httpServletResponse.getWriter.println("<tr>")
      httpServletResponse.getWriter.println("<td>" + c.name + "</td>")
      httpServletResponse.getWriter.println("<td>" + c.hair + "</td>")
      httpServletResponse.getWriter.println("<td>" + c.beard + "</td>")
      httpServletResponse.getWriter.println("</tr>")
         i += 1
    }
    httpServletResponse.getWriter.println("</table>")
    httpServletResponse.getWriter.println(" committers")
  }
}
