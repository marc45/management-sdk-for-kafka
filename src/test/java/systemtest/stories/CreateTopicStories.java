/**
 * Copyright (c) 2017 McAfee LLC - All Rights Reserved
 */

package systemtest.stories;

import org.jbehave.core.configuration.Configuration;
import org.jbehave.core.junit.JUnitStories;
import org.jbehave.core.reporters.StoryReporterBuilder;
import org.jbehave.core.steps.InjectableStepsFactory;
import org.jbehave.core.steps.InstanceStepsFactory;
import systemtest.steps.CreateTopicSteps;

import java.util.Arrays;
import java.util.List;

import static org.jbehave.core.reporters.Format.ANSI_CONSOLE;
import static org.jbehave.core.reporters.Format.HTML;
import static org.jbehave.core.reporters.Format.STATS;

public class CreateTopicStories extends JUnitStories {

    @Override
    public Configuration configuration() {
        return super.configuration()
                .useStoryReporterBuilder(
                        new StoryReporterBuilder()
                                .withDefaultFormats()
                                .withFormats(ANSI_CONSOLE, STATS, HTML));
    }

    // Here we specify the steps classes
    @Override
    public InjectableStepsFactory stepsFactory() {
        return new InstanceStepsFactory(configuration(), new CreateTopicSteps());
    }


    @Override
    protected List<String> storyPaths() {
        return Arrays.asList("CreateTopicStories.story");
    }
}
