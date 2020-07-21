<%@ page import="java.util.List" %>
<%@ page import="pps.ppapp.Report" %>
<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <title>Report</title>
</head>
<body>
    <p><a href="${pageContext.request.contextPath}/ppapp.jsp">Home</a>
       <a href="${pageContext.request.contextPath}/logout">Logout</a>
    <div style="text-align: center;">
    <%
        String title = (String) session.getAttribute("ppapp-report-title");
    %>
    <h2>Отчёт <% out.println(title); %></h2>
    <%
    Report report = (Report) session.getAttribute("ppapp-report");
    if (report != null) {
        out.println("<table border=\"1\">");
        out.println("<thead>");
        out.println("<tr>");
        List<String> head = report.getHead();
        for (String name : head) {
            out.print("<th>" + name + "</th>");
        }
        out.println("</tr>");
        out.println("</thead>");

        out.println("<tbody>");
        List<List<String>> data = report.getData();
        for (List<String> r : data) {
            out.println("<tr>");
            for (String v: r) {
                out.print("<td>" + v + "</td>");
            }
            System.out.println();
            out.println("</tr>");
        }
        out.println("</tbody>");

        out.println("</table>");
    }
    %>
</body>
</html>
