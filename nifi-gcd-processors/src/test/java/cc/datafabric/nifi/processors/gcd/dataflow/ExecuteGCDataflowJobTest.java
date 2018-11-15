package cc.datafabric.nifi.processors.gcd.dataflow;

import cc.datafabric.nifi.gcd.service.GCDataflowService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.LaunchTemplateResponse;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ExecuteGCDataflowJobTest {

    private TestRunner controller;

    private Dataflow dataflowService;

    private GCDataflowService dataflowServiceProvider;

    private Dataflow.Projects.Locations.Templates.Launch launch;

    private LaunchTemplateResponse launchTemplateResponse;

    //reference values
    private static final String JOB_NAME_VALUE = "job name";
    private static final String JOB_TEMPLATE_PATH_VALUE = "/path/to/template";

    private Random random = new Random();

    @Before
    public void init() throws IOException, InitializationException {
        dataflowService = mock(Dataflow.class, Mockito.RETURNS_DEEP_STUBS);
        launch = mock(Dataflow.Projects.Locations.Templates.Launch.class);
        launchTemplateResponse = mock(LaunchTemplateResponse.class);

        dataflowServiceProvider = new MockGCDataflowService();


        controller = TestRunners.newTestRunner(ExecuteGCDataflowJob.class);

        controller.addControllerService("service", dataflowServiceProvider);
        controller.enableControllerService(dataflowServiceProvider);

        controller.setProperty(ExecuteGCDataflowJob.DATAFLOW_SERVICE, "service");
        controller.setProperty(ExecuteGCDataflowJob.PROJECT_ID, "project name");
        controller.setProperty(ExecuteGCDataflowJob.JOB_NAME, JOB_NAME_VALUE);
        controller.setProperty(ExecuteGCDataflowJob.GCS_PATH, JOB_TEMPLATE_PATH_VALUE);

        when(dataflowService.projects().locations().templates().launch(
                any(String.class),
                any(String.class),
                any(LaunchTemplateParameters.class))
        ).thenReturn(launch);

        when(launch.execute()).thenReturn(launchTemplateResponse);

        controller.addConnection(ExecuteGCDataflowJob.REL_INPROCESS);
        controller.addConnection(ExecuteGCDataflowJob.REL_NOTIFY);
        controller.addConnection(ExecuteGCDataflowJob.REL_FAILURE);
        controller.addConnection(ExecuteGCDataflowJob.REL_SUCCESS);
    }

    public class MockGCDataflowService extends AbstractControllerService implements GCDataflowService {
        @Override
        public Dataflow getDataflowService() {
            return dataflowService;
        }
    }

    @Test
    public void testJobDoneAndChain() throws IOException {
        //mock the job
        String jobID = "id";
        Job jobMock = mock(Job.class);
        when(jobMock.getId()).thenReturn(jobID);
        when(jobMock.getCurrentState()).thenReturn("JOB_STATE_DONE");

        //mock incoming flow file
        MockFlowFile incomingFlowFile = new MockFlowFile(random.nextLong());
        Map<String, String> attribures = new HashMap<>();
        attribures.put(ExecuteGCDataflowJob.JOB_ID_ATTR, jobID);
        attribures.put(ExecuteGCDataflowJob.JOB_NAME_ATTR, JOB_NAME_VALUE);
        incomingFlowFile.putAttributes(attribures);

        //mock the Dataflow service
        Dataflow.Projects.Locations.Jobs.Get getJobRequest = mock(Dataflow.Projects.Locations.Jobs.Get.class);
        when(dataflowService.projects().locations().jobs().get(
                any(String.class),
                any(String.class),
                eq(jobID))
        ).thenReturn(getJobRequest);
        when(getJobRequest.execute()).thenReturn(jobMock);

        //init processor for the given test
        controller.setProperty(ExecuteGCDataflowJob.VALIDATE_ONLY, "false");
        //push some flow file to run the processor
        controller.enqueue(incomingFlowFile);

        //start processor
        controller.run();

        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 1);
        MockFlowFile success = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_SUCCESS).get(0);
        controller.assertAllFlowFiles(
                ExecuteGCDataflowJob.REL_SUCCESS,
                f ->
                {
                    assertNull(f.getAttribute(ExecuteGCDataflowJob.JOB_ID_ATTR));
                    assertNull(f.getAttribute(ExecuteGCDataflowJob.JOB_NAME_ATTR));
                }
        );


        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 1);
        List<MockFlowFile> notifications = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_NOTIFY);
        MockFlowFile notification = notifications.get(0);
        String content = new String(controller.getContentAsByteArray(notification));
        NotificationMessage notificationMessage = (new ObjectMapper()).readerFor(NotificationMessage.class).readValue(content);
        assertEquals(ExecuteGCDataflowJob.JOB_STATE_DONE, notificationMessage.getState());

        //drop flow files as if some following processors have ingested them
        controller.clearTransferState();

        jobLaunch(jobID, success);
    }


    @Test
    public void testJobLaunch() throws IOException {
        jobLaunch("id", new MockFlowFile(random.nextLong()));
    }

    private void jobLaunch(String jobID, MockFlowFile incomingFlowFile) throws IOException {
        Job jobMock = mock(Job.class);
        when(jobMock.getId()).thenReturn(jobID);
        when(launchTemplateResponse.getJob()).thenReturn(jobMock);

        controller.setProperty(ExecuteGCDataflowJob.VALIDATE_ONLY, "false");
        //push some flow file to run the processor
        controller.enqueue(incomingFlowFile);

        controller.run();

        List<MockFlowFile> notifications = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_NOTIFY);
        assertTrue(!notifications.isEmpty());
        for (MockFlowFile flowFile : notifications) {
            String content = new String(controller.getContentAsByteArray(flowFile));
            NotificationMessage notificationMessage = (new ObjectMapper()).readerFor(NotificationMessage.class).readValue(content);
            assertEquals(ExecuteGCDataflowJob.JOB_STATE_ON_START, notificationMessage.getState());
            assertEquals(JOB_TEMPLATE_PATH_VALUE, notificationMessage.getFields().get(ExecuteGCDataflowJob.JOB_TEMPLATE_PATH_NF_FIELD));
            assertEquals(Boolean.FALSE.toString(), notificationMessage.getFields().get(ExecuteGCDataflowJob.JOB_DRY_RUN_NF_FIELD));
            assertEquals(JOB_NAME_VALUE, notificationMessage.getFields().get(ExecuteGCDataflowJob.JOB_NAME_NF_FIELD));
        }

        //check that processor has launched and pushed flow file to 'inprocess' queue to use it for subsequent job polling
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 1);
        controller.assertAllFlowFiles(
                ExecuteGCDataflowJob.REL_INPROCESS,
                f ->
                {
                    assertEquals(jobID, f.getAttribute(ExecuteGCDataflowJob.JOB_ID_ATTR));
                    assertEquals(JOB_NAME_VALUE, f.getAttribute(ExecuteGCDataflowJob.JOB_NAME_ATTR));
                }
        );

        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);

    }




    @Test
    public void testJobLaunchAndDone() throws IOException {
        String intialState = ExecuteGCDataflowJob.JOB_STATE_PENDING;
        String[] states = new String[]{
                ExecuteGCDataflowJob.JOB_STATE_RUNNING,
                ExecuteGCDataflowJob.JOB_STATE_DONE
        };

        initJob(intialState, states);

        controller.enqueue("");

        controller.run();
        MockFlowFile inprocess = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_INPROCESS).get(0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);
        controller.clearTransferState();

        controller.enqueue(inprocess);
        controller.run();
        inprocess = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_INPROCESS).get(0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);
        controller.clearTransferState();

        controller.enqueue(inprocess);
        controller.run();
        inprocess = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_INPROCESS).get(0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);
        controller.clearTransferState();

        controller.enqueue(inprocess);
        controller.run();
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 1);
        controller.clearTransferState();

    }

    @Test
    public void testJobRunning() throws IOException {
        String initialState = ExecuteGCDataflowJob.JOB_STATE_PENDING;
        String[] states = new String[]{
                ExecuteGCDataflowJob.JOB_STATE_RUNNING,
                ExecuteGCDataflowJob.JOB_STATE_CANCELLING,
                ExecuteGCDataflowJob.JOB_STATE_CANCELLED
        };

        initJob(initialState, states);

        controller.enqueue("");

        controller.run();
        MockFlowFile inprocess = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_INPROCESS).get(0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);
        controller.clearTransferState();

        controller.enqueue(inprocess);
        controller.run();
        inprocess = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_INPROCESS).get(0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);
        controller.clearTransferState();

        controller.enqueue(inprocess);
        controller.run();
        inprocess = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_INPROCESS).get(0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);
        controller.clearTransferState();

        controller.enqueue(inprocess);
        controller.run();
        inprocess = controller.getFlowFilesForRelationship(ExecuteGCDataflowJob.REL_INPROCESS).get(0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);
        controller.clearTransferState();

        controller.enqueue(inprocess);
        controller.run();
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_INPROCESS, 0);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_NOTIFY, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_FAILURE, 1);
        controller.assertTransferCount(ExecuteGCDataflowJob.REL_SUCCESS, 0);

        controller.assertAllFlowFiles(
                ExecuteGCDataflowJob.REL_FAILURE,
                f ->
                {
                    assertNull(f.getAttribute(ExecuteGCDataflowJob.JOB_ID_ATTR));
                    assertNull(f.getAttribute(ExecuteGCDataflowJob.JOB_NAME_ATTR));
                }
        );
    }

    private void initJob(String initialState, String[] states) throws IOException {
        Job job = mock(Job.class);
        when(job.getId()).thenReturn("id");


        Dataflow.Projects.Locations.Jobs.Get getJobRequest = mock(Dataflow.Projects.Locations.Jobs.Get.class);
        when(dataflowService.projects().locations().jobs().get(
                any(String.class),
                any(String.class),
                eq("id"))
        ).thenReturn(getJobRequest);

        when(getJobRequest.execute()).thenReturn(job);
        when(job.getCurrentState()).thenReturn(initialState, states);

        when(launchTemplateResponse.getJob()).thenReturn(job);
    }

}

