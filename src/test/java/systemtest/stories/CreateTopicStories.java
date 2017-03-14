/**
 * Copyright (c) 2017 McAfee Inc. - All Rights Reserved
 */

package systemtest.stories;

import org.jbehave.core.configuration.Configuration;
import org.jbehave.core.junit.JUnitStories;
import org.jbehave.core.reporters.Format;
import org.jbehave.core.reporters.StoryReporterBuilder;
import org.jbehave.core.steps.InjectableStepsFactory;
import org.jbehave.core.steps.InstanceStepsFactory;
import systemtest.steps.KafkaMonitorSteps;
import systemtest.steps.ManagementTopicSteps;
import systemtest.steps.ZookeeperMonitorSteps;

import java.util.Arrays;
import java.util.List;

import static org.jbehave.core.reporters.Format.CONSOLE;
import static org.jbehave.core.reporters.Format.TXT;

public class CreateTopicStories extends JUnitStories {

    @Override
    public Configuration configuration() {
        return super.configuration()
                .useStoryReporterBuilder(
                        new StoryReporterBuilder()
                                .withDefaultFormats()
                                .withFormats(Format.HTML,CONSOLE, TXT));
    }

    // Here we specify the steps classes
    @Override
    public InjectableStepsFactory stepsFactory() {
        return new InstanceStepsFactory(configuration(), new ManagementTopicSteps());
    }


    @Override
    protected List<String> storyPaths() {
        return Arrays.asList("./CreateTopicStories.story");
    }
}
