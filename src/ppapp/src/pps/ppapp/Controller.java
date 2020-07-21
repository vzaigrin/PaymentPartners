package pps.ppapp;

import javax.servlet.http.*;
import java.io.IOException;

public class Controller extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        proceed(request, response);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        proceed(request, response);
    }

    protected void proceed(HttpServletRequest request, HttpServletResponse response) throws IOException {
        HttpSession session = request.getSession();
        Object sid = session.getAttribute("ppapp-sid");

        try {
            switch (request.getRequestURI()) {
                case "/login":
                    String username = request.getParameter("username");
                    String password = request.getParameter("password");
                    if (Model.getInstance().validateUser(username, password)) {
                        // Логин и пароль правильные, устанавливаем аттрибуты сессии
                        session.setAttribute("ppapp-sid", Model.getInstance().addSession(username));
                        session.setAttribute("ppapp-user", username);
                        session.setAttribute("ppapp-role", Model.getInstance().getUserRole(username));
                        response.sendRedirect(request.getContextPath() + "/ppapp.jsp");
                    } else {
                        response.sendRedirect(request.getContextPath() + "/login.html");
                    }
                    break;
                case "/logout":
                    Model.getInstance().closeSession(sid.toString());
                    session.invalidate();
                    response.sendRedirect(request.getContextPath() + "/index.html");
                    break;
                case "/report":
                    String reportName = request.getParameter("name");
                    String reportTitle = request.getParameter("title");
                    Object userObject = session.getAttribute("ppapp-user");
                    Object roleObject = session.getAttribute("ppapp-role");
                    if (userObject != null && roleObject != null) {
                        Report report = Model.getInstance().getReport(reportName, userObject.toString(), roleObject.toString());
                        session.setAttribute("ppapp-report-name", reportName);
                        session.setAttribute("ppapp-report-title", reportTitle);
                        session.setAttribute("ppapp-report", report);
                        response.sendRedirect(request.getContextPath() + "/report.jsp");
                    } else
                        response.sendRedirect(request.getContextPath() + "/ppapp.jsp");
                    break;
                default:
                    // Проверяем сессию
                    if (sid != null && Model.getInstance().validateSession(sid.toString())) {
                        // Сессия активна
                        response.sendRedirect(request.getContextPath() + "/ppapp.jsp");
                    } else {
                        // Сессия неактивна
                        response.sendRedirect(request.getContextPath() + "/index.html");
                    }
            }
        } catch (Exception e) {
            response.getWriter().println(e.getMessage());
        }
    }
}
