package cc.datafabric.nifi.gcd.service;

import com.google.api.services.dataflow.Dataflow;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;


@Tags({"gcd", "datflow", "service", "provider"})
@CapabilityDescription("Provides dataflow service.")
public interface GCDataflowService extends ControllerService {
    Dataflow getDataflowService();
}

