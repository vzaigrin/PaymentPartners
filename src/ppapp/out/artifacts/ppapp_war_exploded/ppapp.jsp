<%@ page import="pps.ppapp.Model" %>
<%@ page import="java.util.HashMap" %>
<%@ page import="java.util.Map" %>
<%@ page contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <meta charset="utf-8">
    <title>Analytical tool for merchants</title>
</head>
<body>
    <div style="text-align: center;">
    <%
        String user = (String) session.getAttribute("ppapp-user");
    %>
    <h2>Welcome, <% out.println(user); %></h2>
    </div>
    <p><strong>Отчёты:</strong>
    <ul>
    <%
        HashMap<String, String> reports = Model.getInstance().getReports();
        for (Map.Entry<String, String> entry : reports.entrySet())
            out.println("<li><a href=\"/report?name=" + entry.getValue() + "&title=" + entry.getKey() + "\">" + entry.getKey() + "</a></li>");
    %>
    </ul>
    <p><a href="${pageContext.request.contextPath}/logout">Logout</a></p>
</body>
</html>
