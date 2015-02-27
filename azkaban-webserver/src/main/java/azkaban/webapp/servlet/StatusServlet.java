package azkaban.webapp.servlet;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutableNode;
import azkaban.executor.ExecutorManager;
import azkaban.executor.ExecutorManagerException;
import azkaban.flow.Flow;
import azkaban.flow.FlowProps;
import azkaban.project.Project;
import azkaban.project.ProjectManager;
import azkaban.project.ProjectManagerException;
import azkaban.utils.Props;
import azkaban.webapp.AzkabanWebServer;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by fzawadiak on 21.10.14.
 */
public class StatusServlet extends AbstractAzkabanServlet {
    @java.lang.Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        if(!hasParam(req,"project")) {
            throw new ServletException("Project parameter required");
        }

        ServletOutputStream out = resp.getOutputStream();
        AzkabanWebServer server = (AzkabanWebServer) getApplication();
        ProjectManager projectManager = server.getProjectManager();
        ExecutorManager executorManager = server.getExecutorManager();

        String projectName = getParam(req,"project");
        Project project = projectManager.getProject(projectName);

        if(project==null) {
            throw new ServletException("Project not found");
        }

        resp.setContentType("text/xml");
        resp.addHeader("Cache-Control","no-cache");

        out.println("<?xml version='1.0' encoding='UTF-8'?>");
        out.println("<project name='" + project.getName() + "'>");
        for(Flow flow : project.getFlows()) {
            try {
                /*
                Map<String, FlowProps> all = flow.getAllFlowProps();
                for(String name : all.keySet()) {
                    out.println("\t<propset name='"+name+"'>");
                    Props props = projectManager.getProperties(project,name);
                    for (String prop : props.getKeySet()) {
                        String val = props.getString(prop);
                        out.println("\t\t<prop name='" + prop + "' value='" + val + "' />");
                    }
                    out.println("\t</propset>");
                }
                */
                List<ExecutableFlow> flows = executorManager.getExecutableFlows(project,flow.getId(),0,1);
                if(flows.isEmpty()) {
                    out.println("\t<flow name='" + flow.getId() + "' status='SKIPPED'/>");
                    continue;
                }

                ExecutableFlow ef = flows.get(0);
                out.println("\t<flow name='" + ef.getId() + "' status='" + ef.getStatus() + "' time='" + new Date(ef.getStartTime()).toString() + "'>");
                for(ExecutableNode node : ef.getExecutableNodes()) {
                    long elapsed = node.getEndTime() - node.getStartTime();
                    out.println("\t\t<job name='" + node.getId() + "' elapsed='" + elapsed + "' status='" + node.getStatus() + "'/>");
                }
                out.println("\t</flow>");
            } catch(ExecutorManagerException e) {
                throw(new ServletException(e));
            } /*catch(ProjectManagerException e) {
                throw(new ServletException(e));
            }*/
        }
        out.println("</project>");
    }
}
