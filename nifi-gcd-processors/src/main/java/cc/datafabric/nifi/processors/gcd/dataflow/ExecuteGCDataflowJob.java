package cc.datafabric.nifi.processors.gcd.dataflow;

import cc.datafabric.nifi.gcd.service.GCDataflowService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.LaunchTemplateResponse;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.*;


@Tags({"google", "google cloud platform", "dataflow", "put"})
@CapabilityDescription("Launches GCP Dataflow job based on the specified template and monitors its execution. The standard use case assumes: 1) connection of 'inprocess' relation with the processor itself. Flowfiles on this relation are used to re-ask GCP Dataflow service about the job state; 2) connect 'notify' relation with a processor that can re-send notification to some external system, e.g. to PutSlack processor.")
@WritesAttributes({
        @WritesAttribute(attribute = "job.id", description = "Id of the job assigned by GCP Dataflow service. The attribute is set only for flowfiles emitted from 'inprocess' relation. Presence of this attribute will make the processor to ask GCP Dataflow service about the job with this id."),
        @WritesAttribute(attribute = "job.state", description = "The state of the job as reported by GCP Dataflow service. The attribute is set only for flowfiles emitted from 'inprocess' relation."),
        @WritesAttribute(attribute = "job.name", description = "Name of the job as it has been set by the user. The attribute is set only for flowfiles emitted from 'inprocess' relation.")
})

@DynamicProperty(name = "The name of a User-Defined Parameters to be sent to the job",
        value = "The value of a User-Defined parameter to be set to the job",
        description = "Allows User-Defined parameter to be sent to the job as a key/value pair. To make job template program written in Apache Beam primitives to accept those parameters it is necessary to declare them in pipeline options and use ValueProvider construct to read them inside pipeline code.",
        supportsExpressionLanguage = true)
public class ExecuteGCDataflowJob extends AbstractProcessor {

    public static final PropertyDescriptor DATAFLOW_SERVICE = new PropertyDescriptor.Builder()
            .name("dataflow-service-provider")
            .displayName("GCP Dataflow Service Provider")
            .description("The Controller Service that is used to provide GCP Dataflow service.")
            .required(true)
            .identifiesControllerService(GCDataflowService.class)
            .build();

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor
            .Builder().name("gcp-project-id")
            .displayName("Project ID")
            .description("Google Cloud Project ID.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor JOB_NAME = new PropertyDescriptor
            .Builder().name("job-name")
            .displayName("Job name")
            .description("Name of the job to be created.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor GCS_PATH = new PropertyDescriptor
            .Builder().name("gcs-path")
            .displayName("GCS path")
            .description("Google Cloud Storage path to the template of the job.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor VALIDATE_ONLY = new PropertyDescriptor
            .Builder().name("validate-only")
            .displayName("Validate only attribute")
            .description("If true, the request is validated but not actually executed. Default is true.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor LOCATION = new PropertyDescriptor
            .Builder().name("location")
            .displayName("Job location")
            .description("Enables to specify where it is necessary to place the job.")
            .required(true)
            .allowableValues("europe-west1", "us-central1", "us-west1")
            .defaultValue("europe-west1")
            .build();

    public static final PropertyDescriptor MACHINE_TYPES = new PropertyDescriptor
            .Builder().name("machine-type")
            .displayName("Machine type")
            .description("Enables to specify type of the machine to run the job. By default 'n1-standard-1' is used.")
            .required(true)
            .allowableValues
                    (
                            "n1-standard-1",
                            "n1-standard-2",
                            "n1-standard-4",
                            "n1-standard-8",
                            "n1-standard-16",
                            "n1-standard-32",
                            "n1-standard-64",
                            "n1-standard-96",
                            "n1-highmem-2",
                            "n1-highmem-4",
                            "n1-highmem-8",
                            "n1-highmem-16",
                            "n1-highmem-32",
                            "n1-highmem-64",
                            "n1-highmem-96",
                            "n1-highcpu-2",
                            "n1-highcpu-4",
                            "n1-highcpu-8",
                            "n1-highcpu-16",
                            "n1-highcpu-32",
                            "n1-highcpu-64",
                            "n1-highcpu-96"
                    )
            .defaultValue("n1-standard-1")
            .build();

    public static final String JOB_ID_ATTR = "job.id";
    public static final String JOB_STATE_ATTR = "job.state";
    public static final String JOB_NAME_ATTR = "job.name";

    //standard GC Dataflow states
    public static final String JOB_STATE_UNKNOWN = "JOB_STATE_UNKNOWN";
    public static final String JOB_STATE_STOPPED = "JOB_STATE_STOPPED";
    public static final String JOB_STATE_RUNNING = "JOB_STATE_RUNNING";
    public static final String JOB_STATE_DONE = "JOB_STATE_DONE";
    public static final String JOB_STATE_FAILED = "JOB_STATE_FAILED";
    public static final String JOB_STATE_CANCELLED = "JOB_STATE_CANCELLED";
    public static final String JOB_STATE_UPDATED = "JOB_STATE_UPDATED";
    public static final String JOB_STATE_DRAINING = "JOB_STATE_DRAINING";
    public static final String JOB_STATE_DRAINED = "JOB_STATE_DRAINED";
    public static final String JOB_STATE_PENDING = "JOB_STATE_PENDING";
    public static final String JOB_STATE_CANCELLING = "JOB_STATE_CANCELLING";

    //standard GC Dataflow job states are extended with these states to signal about job launching and its failure
    public static final String JOB_STATE_ON_START = "JOB_STATE_ON_START";
    public static final String JOB_STATE_LAUNCH_FAILED = "JOB_STATE_LAUNCH_FAILED";

    //some standard message fields, there may be some arbitrary ones that are taken from dynamic properties
    public static final String JOB_NAME_NF_FIELD = "job name";
    public static final String JOB_ID_NF_FIELD = "job id";
    public static final String JOB_TEMPLATE_PATH_NF_FIELD = "template path";
    public static final String JOB_DRY_RUN_NF_FIELD = "dry run";
    public static final String PROCESS_NAME_NF_FIELD = "process name";
    public static final String ERROR_MESSAGE_NF_FIELD = "error message";

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .add(DATAFLOW_SERVICE)
                .add(PROJECT_ID)
                .add(LOCATION)
                .add(MACHINE_TYPES)
                .add(JOB_NAME)
                .add(GCS_PATH)
                .add(VALIDATE_ONLY)
                .build();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success")
                    .description("FlowFiles are routed to this relationship after GCP Dataflow service has reported that the job was successfully done.")
                    .build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("FlowFiles are routed to this relationship after GCP Dataflow service has reported that the job was failed, cancelled or got an unknown state, or because of an internal error within the processor.")
                    .build();

    public static final Relationship REL_INPROCESS =
            new Relationship.Builder().name("inprocess")
                    .description("Received flow files are 'circulating' within this relation until the moment the job is done or failed.")
                    .build();

    public static final Relationship REL_NOTIFY =
            new Relationship.Builder().name("notify")
                    .description("For this relation new flowfiles are generated. They contain information that can be used for notification about the job state.")
                    .build();

    public static Set<Relationship> relationships;


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_INPROCESS);
        relationships.add(REL_NOTIFY);
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        try {
            Dataflow dataflowService = context
                    .getProperty(DATAFLOW_SERVICE)
                    .asControllerService(GCDataflowService.class)
                    .getDataflowService();

            if (flowFile == null) {
                return;
            } else {
                String jobId = flowFile.getAttribute(JOB_ID_ATTR);
                if (jobId == null) {
                    launchJob(dataflowService, context, session, flowFile);
                } else if (!jobId.isEmpty()) {
                    examineJobs(context, session, dataflowService, flowFile);
                } else {
                    getLogger().error("FlowFile has id field but it is empty!");
                    emitFailures(
                            context,
                            session,
                            flowFile,
                            "FlowFile has id field but it is empty!"
                    );
                }
            }
        } catch (final GoogleJsonResponseException e) {
            getLogger().error("Failed to launch job due to ", e);
            emitFailures(context, session, flowFile, e.getDetails().getMessage());
        } catch (final Exception e) {
            getLogger().error("Failed to launch job due to ", e);
            emitFailures(context, session, flowFile, e.getMessage());
        }
    }

    private void launchJob(
            Dataflow dataflowService,
            ProcessContext context,
            ProcessSession session,
            FlowFile flowFile
    ) throws IOException {

        LaunchTemplateParameters launchTemplateParameters = new LaunchTemplateParameters();

        RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();
        runtimeEnvironment.setMachineType(context.getProperty(MACHINE_TYPES).getValue());
        launchTemplateParameters.setEnvironment(runtimeEnvironment);

        String jobName = context.getProperty(JOB_NAME)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        launchTemplateParameters.setJobName(jobName);


        Map<String, String> parameters = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                final String value = context
                        .getProperty(entry.getKey())
                        .evaluateAttributeExpressions(flowFile)
                        .getValue();
                parameters.put(entry.getKey().getName(), value);
            }
        }
        launchTemplateParameters.setParameters(parameters);

        String projectId = context.getProperty(PROJECT_ID)
                .evaluateAttributeExpressions(flowFile)
                .getValue();


        Dataflow.Projects.Locations.Templates.Launch launch = dataflowService
                .projects()
                .locations()
                .templates()
                .launch(projectId, context.getProperty(LOCATION).getValue(), launchTemplateParameters);

        String gcpPath = context.getProperty(GCS_PATH)
                .evaluateAttributeExpressions(flowFile)
                .getValue();
        launch.setGcsPath(gcpPath);

        launch.setValidateOnly(context.getProperty(VALIDATE_ONLY).asBoolean());

        LaunchTemplateResponse launchResponse = launch.execute();

        if (!launch.getValidateOnly()) {
            if (launchResponse != null && launchResponse.getJob() != null && launchResponse.getJob().getId() != null) {
                session.transfer(
                        buildFlowFile(context, session, flowFile, launchResponse.getJob().getId()),
                        REL_INPROCESS
                );
                emitNotification(session, launchPreparedNotification(context, parameters, flowFile, JOB_STATE_ON_START));
            } else {
                throw new ProcessException("Get null while job launching.");
            }
        } else {
            session.transfer(flowFile, REL_SUCCESS);
            emitNotification(session, launchPreparedNotification(context, parameters, flowFile, JOB_STATE_DONE));
        }
    }

    private void examineJobs(
            ProcessContext context,
            ProcessSession session,
            Dataflow dataflowService,
            FlowFile flowFile
    ) throws IOException {
        String id = flowFile.getAttribute(JOB_ID_ATTR);
        Dataflow.Projects.Locations.Jobs.Get request = dataflowService
                .projects()
                .locations()
                .jobs()
                .get(
                        context.getProperty(PROJECT_ID).evaluateAttributeExpressions(flowFile).getValue(),
                        context.getProperty(LOCATION).getValue(),
                        id
                );

        Job response = request.execute();
        String state = response.getCurrentState();
        if (state == null) {
            //TODO: should we count attempts?
            session.transfer(flowFile, REL_INPROCESS);
            return;
        }
        session.putAttribute(flowFile, JOB_STATE_ATTR, state);

        switch (state) {
            case JOB_STATE_RUNNING:
            case JOB_STATE_PENDING:
            case JOB_STATE_CANCELLING:
                session.transfer(flowFile, REL_INPROCESS);
                break;
            case JOB_STATE_DONE: {
                getLogger().info("The job with id " + id + " has been done!");
                emitNotification(session, jobStateNotification(state, flowFile));
                session.getProvenanceReporter().create(flowFile, "It takes something about"
                        + response.getCurrentStateTime()
                        + " to perform the job");
                flowFile = removeObsoleteAttributes(session, flowFile);
                session.transfer(flowFile, REL_SUCCESS);
                break;
            }
            case JOB_STATE_UNKNOWN:
            case JOB_STATE_FAILED:
            case JOB_STATE_CANCELLED: {
                getLogger().info(
                        "The job with id {} unsuccessfully terminated with the state {}!",
                        new Object[]{id, state}
                );
                emitNotification(session, jobStateNotification(state, flowFile));
                flowFile = removeObsoleteAttributes(session, flowFile);
                session.transfer(flowFile, REL_FAILURE);
                break;
            }
            case JOB_STATE_STOPPED:
                getLogger().info("The job with id " + id + " has been stopped!");
                emitNotification(session, jobStateNotification(state, flowFile));
                session.transfer(flowFile, REL_INPROCESS);
                break;
            default:
                getLogger().error("The job with id {} is in unworkable {} state!", new Object[]{id, state});
                emitNotification(session, jobStateNotification(state, flowFile));
                flowFile = removeObsoleteAttributes(session, flowFile);
                session.transfer(flowFile, REL_FAILURE);
        }
    }

    /**
     * Remove attributes that can be misidentified by the next job control processor as ones that belonging to them,
     * however they have not yet created any job
     *
     * @param session
     * @param flowFile
     * @return
     */
    private FlowFile removeObsoleteAttributes(ProcessSession session, FlowFile flowFile) {
        flowFile = session.removeAllAttributes(
                flowFile,
                new HashSet(Arrays.asList(JOB_ID_ATTR, JOB_STATE_ATTR, JOB_NAME_ATTR))
        );
        return flowFile;
    }

    private void emitNotification(
            ProcessSession session,
            NotificationMessage message
    ) {
        FlowFile notification = null;
        try {
            notification = session.create();
            notification = session.write(
                    notification,
                    (OutputStream outputStream)
                            ->
                            (new ObjectMapper()).writeValue(outputStream, message)
            );
            session.transfer(notification, REL_NOTIFY);
        } catch (Exception e) {
            getLogger().error("Failed to create notification!", e);
            if (notification != null) {
                session.remove(notification);
            }
        }
    }

    private void emitFailures(
            ProcessContext context,
            ProcessSession session,
            FlowFile flowFile,
            String errorMessage
    ) {
        emitNotification(session, jobLaunchFailedNotification(context, errorMessage));
        removeObsoleteAttributes(session, flowFile);
        flowFile = session.penalize(flowFile);
        session.transfer(flowFile, REL_FAILURE);
    }

    private NotificationMessage launchPreparedNotification(
            ProcessContext context,
            Map<String, String> parameters,
            FlowFile flowFile,
            String state
    ) {
        NotificationMessage message = new NotificationMessage();
        message
                .addField(
                        JOB_NAME_NF_FIELD,
                        context.getProperty(JOB_NAME).evaluateAttributeExpressions(flowFile).getValue())
                .addField(
                        JOB_TEMPLATE_PATH_NF_FIELD,
                        context.getProperty(GCS_PATH).evaluateAttributeExpressions(flowFile).getValue())
                .addField(
                        JOB_DRY_RUN_NF_FIELD,
                        context.getProperty(VALIDATE_ONLY).getValue());
        message.setState(state);
        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            message.addField(entry.getKey(), entry.getValue());
        }
        return message;
    }

    private NotificationMessage jobLaunchFailedNotification(
            ProcessContext context,
            String messageStr
    ) {
        NotificationMessage message = new NotificationMessage();
        message
                .addField(PROCESS_NAME_NF_FIELD, context.getName())
                .addField(ERROR_MESSAGE_NF_FIELD, messageStr);
        message.setState(JOB_STATE_LAUNCH_FAILED);
        return message;
    }

    private NotificationMessage jobStateNotification(
            String state,
            FlowFile flowFile
    ) {
        String id = flowFile.getAttribute(JOB_ID_ATTR);
        NotificationMessage message = new NotificationMessage();
        message
                .addField(JOB_ID_NF_FIELD, id)
                .addField(JOB_NAME_NF_FIELD, flowFile.getAttribute(JOB_NAME_ATTR));
        message.setState(state);
        return message;
    }

    private FlowFile buildFlowFile(
            ProcessContext context,
            ProcessSession session,
            FlowFile flowFile,
            String id
    ) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(JOB_ID_ATTR, id);
        attributes.put(
                JOB_NAME_ATTR,
                context.getProperty(JOB_NAME).evaluateAttributeExpressions(flowFile).getValue()
        );
        return session.putAllAttributes(flowFile, attributes);
    }
}

