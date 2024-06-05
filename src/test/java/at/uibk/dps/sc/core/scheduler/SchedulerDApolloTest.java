package at.uibk.dps.sc.core.scheduler;

import at.uibk.dps.ee.guice.starter.VertxProvider;
import at.uibk.dps.ee.io.afcl.AfclReader;
import at.uibk.dps.ee.io.resources.ResourceGraphProviderFile;
import at.uibk.dps.ee.io.spec.SpecificationProviderFile;
import at.uibk.dps.ee.model.graph.ResourceGraphProvider;
import at.uibk.dps.ee.model.graph.SpecificationProvider;
import at.uibk.dps.sc.core.capacity.CapacityCalculatorNone;
import at.uibk.dps.sc.core.scheduler.dApollo.Statistics;
import at.uibk.dps.sc.core.scheduler.dApollo.TestHelper;
import com.google.gson.*;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for the dApollo scheduling algorithm. Test cases include the
 * scheduling of the following workflows: CasaWind, Genome1000, Montage and PSLoad.
 * The test cases are executed for several bandwidth and cost limit values.
 * As a proof of concept for the scheduler, within the test cases each task of the
 * workflow is scheduled to either an edge or a cloud resource.
 */
class SchedulerDApolloTest {

    /**
     * Vertex provider.
     */
    private static VertxProvider vProv;

    /**
     * Setup for all tests.
     */
    @BeforeAll static void setup() {
        Vertx vertx = Vertx.vertx();
        vProv = new VertxProvider(vertx);
    }

    /**
     * Generate the helper list containing test cases.
     *
     * @param filePath of the csv file containing test cases.
     *
     * @return List of test cases.
     *
     * @throws IOException if file not found.
     */
    private List<TestHelper> generateTestHelperList(String filePath) throws IOException {
        List<TestHelper> testHelperList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine();
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                testHelperList.add(new TestHelper(Double.valueOf(values[0]), Double.valueOf(values[1]),Double.valueOf(values[2]), Double.valueOf(values[3])));
            }
        }
        return testHelperList;
    }

    /**
     * Adjust the scheduler input based on the test cases.
     *
     * @param schedulerInput input to adjust.
     *
     * @param th test helper containing test cases.
     */
    private void adjustSchedulerInput(JsonObject schedulerInput, TestHelper th) {
        schedulerInput.add("costLimit", new JsonPrimitive(th.getCostLimit()));
        for (JsonElement taskResource : schedulerInput.get("taskResourceTypes").getAsJsonArray()) {
            JsonObject obj = taskResource.getAsJsonObject();
            if (obj.get("id").getAsString().contains("edge")) {
                obj.add("bandwidth", new JsonPrimitive(th.getBandwidth()));
            }
        }
        for (JsonElement taskResource : schedulerInput.get("RSResourceTypes").getAsJsonArray()) {
            JsonObject obj = taskResource.getAsJsonObject();
            if (obj.get("id").getAsString().contains("edge")) {
                obj.add("bandwidth", new JsonPrimitive(th.getBandwidth()));
            }
        }
    }

    /**
     * Scheduling of the CasaWind workflow.
     */
    @Test void testCasaWind() throws IOException {
        // Setup specification
        AfclReader afclReader = new AfclReader(new File("src/test/resources/CasaWind/workflow.yaml").getAbsolutePath());
        ResourceGraphProvider resourceGraphProvider =
                new ResourceGraphProviderFile(new File("src/test/resources/CasaWind/typemappings.json").getAbsolutePath());
        SpecificationProvider specificationProviderFile = new SpecificationProviderFile(afclReader, resourceGraphProvider,
                new File("src/test/resources/CasaWind/typemappings.json").getAbsolutePath());

        // Configure scheduler
        List<TestHelper> testHelperList = generateTestHelperList("src/test/resources/CasaWind/results.csv");
        JsonObject schedulerInput = new Gson().fromJson(new BufferedReader(new FileReader("src/test/resources/CasaWind/schedulerInput.json")), JsonObject.class);

        int testCase = 1;
        for(TestHelper th: testHelperList) {
            adjustSchedulerInput(schedulerInput, th);
            SchedulerDApollo
                schedulerdApollo = new SchedulerDApollo(specificationProviderFile, schedulerInput, false, new CapacityCalculatorNone(), vProv);

            System.out.println("Test case " + (testCase++) + "/" + testHelperList.size());

            // Schedule workflow tasks
            for (int i = 0; i <= 24; i++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("unzip" + i));
            }
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("max_velocity"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("merged_netcfd2png"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mvt"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("pointalert"));

            // Check results
            Statistics statistics = schedulerdApollo.getStatistics();
            assertEquals(th.getExpectedRuntime(), statistics.getRuntime());
            assertEquals(th.getExpectedCost(), statistics.getCost());
            assertTrue(statistics.getCost() <= th.getCostLimit());
        }
    }

    /**
     * Scheduling of the Genome1000 workflow.
     */
    @Test void testGenome1000() throws IOException {
        // Setup specification
        AfclReader afclReader = new AfclReader(new File("src/test/resources/Genome1000/workflow.yaml").getAbsolutePath());
        ResourceGraphProvider resourceGraphProvider =
                new ResourceGraphProviderFile(new File("src/test/resources/Genome1000/typemappings.json").getAbsolutePath());
        SpecificationProvider specificationProviderFile = new SpecificationProviderFile(afclReader, resourceGraphProvider,
                new File("src/test/resources/Genome1000/typemappings.json").getAbsolutePath());


        // Configure scheduler
        List<TestHelper> testHelperList = generateTestHelperList("src/test/resources/Genome1000/results.csv");
        JsonObject schedulerInput = new Gson().fromJson(new BufferedReader(new FileReader("src/test/resources/Genome1000/schedulerInput.json")), JsonObject.class);

        int testCase = 1;
        for(TestHelper th: testHelperList) {
            adjustSchedulerInput(schedulerInput, th);
            SchedulerDApollo
                schedulerdApollo = new SchedulerDApollo(specificationProviderFile, schedulerInput, false, new CapacityCalculatorNone(), vProv);

            System.out.println("Test case " + (testCase++) + "/" + testHelperList.size());

            // Schedule workflow tasks
            for(int i = 0; i <= 9; i++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("individuals" + i));
            }
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("sifting"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("individualsMerge"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mutOverlapEUR"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mutOverlapAFR"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mutOverlapEAS"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mutOverlapALL"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mutOverlapGBR"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mutOverlapSAS"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mutOverlapAMR"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("frequencyEUR"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("frequencyAFR"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("frequencyEAS"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("frequencyALL"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("frequencyGBR"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("frequencySAS"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("frequencyAMR"));

            // Check results
            Statistics statistics = schedulerdApollo.getStatistics();
            assertEquals(th.getExpectedRuntime(), statistics.getRuntime());
            assertEquals(th.getExpectedCost(), statistics.getCost());
            assertTrue(statistics.getCost() <= th.getCostLimit());
        }
    }

    /**
     * Scheduling of the Montage workflow.
     */
    @Test void testMontage() throws IOException {
        // Setup specification
        AfclReader afclReader = new AfclReader(new File("src/test/resources/Montage/workflow.yaml").getAbsolutePath());
        ResourceGraphProvider resourceGraphProvider =
                new ResourceGraphProviderFile(new File("src/test/resources/Montage/typemappings.json").getAbsolutePath());
        SpecificationProvider specificationProviderFile = new SpecificationProviderFile(afclReader, resourceGraphProvider,
                new File("src/test/resources/Montage/typemappings.json").getAbsolutePath());

        // Configure scheduler
        List<TestHelper> testHelperList = generateTestHelperList("src/test/resources/Montage/results.csv");
        JsonObject schedulerInput = new Gson().fromJson(new BufferedReader(new FileReader("src/test/resources/Montage/schedulerInput.json")), JsonObject.class);

        int testCase = 1;
        for(TestHelper th: testHelperList) {
            adjustSchedulerInput(schedulerInput, th);
            SchedulerDApollo
                schedulerdApollo = new SchedulerDApollo(specificationProviderFile, schedulerInput, false, new CapacityCalculatorNone(), vProv);

            System.out.println("Test case " + (testCase++) + "/" + testHelperList.size());

            // Schedule workflow tasks
            for (int mProjectPP = 0; mProjectPP < 45; mProjectPP++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mProjectPP" + mProjectPP));
            }
            for (int mDiffFit = 0; mDiffFit < 107; mDiffFit++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mDiffFit" + mDiffFit));
            }
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mBgModel"));
            for (int mBackground = 0; mBackground < 45; mBackground++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mBackground" + mBackground));
            }
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mImgShrinkJPEG"));

            // Check results
            Statistics statistics = schedulerdApollo.getStatistics();
            assertEquals(th.getExpectedRuntime(), statistics.getRuntime());
            assertEquals(th.getExpectedCost(), statistics.getCost());
            assertTrue(statistics.getCost() <= th.getCostLimit());
        }
    }

    /**
     * Scheduling of the 0_25 degree Montage workflow.
     */
    @Test void testMontage0_25() throws IOException {

        // Setup specification
        AfclReader afclReader = new AfclReader(new File("src/test/resources/Montage0_25/workflow.yaml").getAbsolutePath());
        ResourceGraphProvider resourceGraphProvider =
                new ResourceGraphProviderFile(new File("src/test/resources/Montage0_25/typemappings.json").getAbsolutePath());
        SpecificationProvider specificationProviderFile = new SpecificationProviderFile(afclReader, resourceGraphProvider,
                new File("src/test/resources/Montage0_25/typemappings.json").getAbsolutePath());

        // Configure scheduler
        List<TestHelper> testHelperList = generateTestHelperList("src/test/resources/Montage0_25/results.csv");
        JsonObject schedulerInput = new Gson().fromJson(new BufferedReader(new FileReader("src/test/resources/Montage0_25/schedulerInput.json")), JsonObject.class);

        int testCase = 1;
        for(TestHelper th: testHelperList) {
            adjustSchedulerInput(schedulerInput, th);
            SchedulerDApollo
                schedulerdApollo = new SchedulerDApollo(specificationProviderFile, schedulerInput, true, new CapacityCalculatorNone(), vProv);

            System.out.println("Test case " + (testCase++) + "/" + testHelperList.size());

            // Schedule workflow tasks
            for (int mProjectPP = 0; mProjectPP < 12; mProjectPP++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mProjectPP" + mProjectPP));
            }
            for (int mDiffFit = 0; mDiffFit < 20; mDiffFit++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mDiffFit" + mDiffFit));
            }
            //schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mConcatFit"));
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mBgModel"));
            for (int mBackground = 0; mBackground < 12; mBackground++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mBackground" + mBackground));
            }
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("mImgShrinkJPEG"));

            // Check results
            Statistics statistics = schedulerdApollo.getStatistics();
            assertEquals(th.getExpectedRuntime(), statistics.getRuntime());
            assertEquals(th.getExpectedCost(), statistics.getCost());
            assertTrue(statistics.getCost() <= th.getCostLimit());
        }
    }

    /**
     * Scheduling of the PSLoad workflow.
     */
    @Test void testPSLoad() throws IOException {

        // Setup specification
        AfclReader afclReader = new AfclReader(new File("src/test/resources/PSLoad/workflow.yaml").getAbsolutePath());
        ResourceGraphProvider resourceGraphProvider =
                new ResourceGraphProviderFile(new File("src/test/resources/PSLoad/typemappings.json").getAbsolutePath());
        SpecificationProvider specificationProviderFile = new SpecificationProviderFile(afclReader, resourceGraphProvider,
                new File("src/test/resources/PSLoad/typemappings.json").getAbsolutePath());

        // Configure scheduler
        List<TestHelper> testHelperList = generateTestHelperList("src/test/resources/PSLoad/results.csv");
        JsonObject schedulerInput = new Gson().fromJson(new BufferedReader(new FileReader("src/test/resources/PSLoad/schedulerInput.json")), JsonObject.class);

        int testCase = 1;
        for (TestHelper th : testHelperList) {
            adjustSchedulerInput(schedulerInput, th);
            SchedulerDApollo
                schedulerdApollo = new SchedulerDApollo(specificationProviderFile, schedulerInput, false, new CapacityCalculatorNone(), vProv);

            System.out.println("Test case " + (testCase++) + "/" + testHelperList.size());

            // Schedule workflow tasks
            final double final_num_preprocess = 80;
            final double final_num_load = 10;
            for (int num_preprocess = 1; num_preprocess <= final_num_preprocess; num_preprocess++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("preprocess" + num_preprocess));
            }
            for (int num_preprocess = 1; num_preprocess <= final_num_preprocess; num_preprocess++) {
                for (int i = 1; i <= final_num_load; i++) {
                    schedulerdApollo
                        .schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("load_" + num_preprocess + "_" + i));
                }
            }
            for (int num_preprocess = 1; num_preprocess <= final_num_preprocess; num_preprocess++) {
                schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("validate" + num_preprocess));
            }
            schedulerdApollo.schedule(specificationProviderFile.getSpecification().getEnactmentGraph().getVertex("end"));

            // Check results
            Statistics statistics = schedulerdApollo.getStatistics();
            assertEquals(th.getExpectedRuntime(), statistics.getRuntime());
            assertEquals(th.getExpectedCost(), statistics.getCost());
            assertTrue(statistics.getCost() <= th.getCostLimit());
        }
    }
}
