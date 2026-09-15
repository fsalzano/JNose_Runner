import io.github.arieslab.core.Config;
import io.github.arieslab.core.JNoseCore;
import io.github.arieslab.dto.TestClass;
import io.github.arieslab.dto.TestSmell;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JNoseBatchRunner {

    private static final List<String> SMELLS = List.of(
        "AR", "CTL", "CI", "DT", "DpT",
        "DA", "ET", "EmT", "ECT", "GF",
        "IgT", "LT", "MNT", "MG", "PS",
        "RA", "RO", "SE", "ST", "VT", "UT"
    );

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                "Usage: JNoseBatchRunner <project-path> <output-directory>"
            );
            System.exit(1);
        }

        String projectPath = args[0];
        Path outputDir = Paths.get(args[1]);

        Files.createDirectories(outputDir);

        Config config = new Config() {
            public boolean assertionRoulette() { return true; }
            public boolean conditionalTestLogic() { return true; }
            public boolean constructorInitialization() { return true; }
            public boolean defaultTest() { return true; }
            public boolean dependentTest() { return true; }
            public boolean duplicateAssert() { return true; }
            public boolean eagerTest() { return true; }
            public boolean emptyTest() { return true; }
            public boolean exceptionCatchingThrowing() { return true; }
            public boolean generalFixture() { return true; }
            public boolean mysteryGuest() { return true; }
            public boolean printStatement() { return true; }
            public boolean redundantAssertion() { return true; }
            public boolean sensitiveEquality() { return true; }
            public boolean verboseTest() { return true; }
            public boolean sleepyTest() { return true; }
            public boolean lazyTest() { return true; }
            public boolean unknownTest() { return true; }
            public boolean ignoredTest() { return true; }
            public boolean resourceOptimism() { return true; }
            public boolean magicNumberTest() { return true; }

            public int maxStatements() { return 50; }
        };

        JNoseCore jnose = new JNoseCore(config);

        System.out.println("Analyzing project: " + projectPath);

        List<TestClass> classes = jnose.getFilesTest(projectPath);

        System.out.println("Detected test classes: " + classes.size());

        writeOccurrences(
            classes,
            outputDir.resolve("test_smells.csv")
        );

        writeSummary(
            classes,
            outputDir.resolve("test_class_summary.csv")
        );

        System.out.println("Analysis completed.");
        System.out.println(
            "Output directory: " + outputDir.toAbsolutePath()
        );
    }

    private static void writeOccurrences(
        List<TestClass> classes,
        Path output
    ) throws Exception {

        try (PrintWriter out =
                 new PrintWriter(Files.newBufferedWriter(output))) {

            out.println(
                "test_class,full_name,junit_version," +
                "production_file,smell,method,range"
            );

            for (TestClass tc : classes) {

                if (tc.getListTestSmell() == null)
                    continue;

                for (TestSmell smell : tc.getListTestSmell()) {

                    out.println(
                        csv(tc.getName()) + "," +
                        csv(tc.getFullName()) + "," +
                        csv(tc.getJunitVersion()) + "," +
                        csv(tc.getProductionFile()) + "," +
                        csv(smell.getName()) + "," +
                        csv(smell.getMethod()) + "," +
                        csv(smell.getRange())
                    );
                }
            }
        }
    }

    private static void writeSummary(
        List<TestClass> classes,
        Path output
    ) throws Exception {

        try (PrintWriter out =
                 new PrintWriter(Files.newBufferedWriter(output))) {

            out.print(
                "test_class,full_name,junit_version," +
                "production_file,loc,methods"
            );

            for (String smell : SMELLS)
                out.print("," + smell);

            out.println();

            for (TestClass tc : classes) {

                Map<String, Integer> counts =
                    new LinkedHashMap<>();

                for (String smell : SMELLS)
                    counts.put(smell, 0);

                if (tc.getListTestSmell() != null) {

                    for (TestSmell smell : tc.getListTestSmell()) {

                        String key =
                            canonicalSmell(smell.getName());

                        if (key != null) {
                            counts.put(
                                key,
                                counts.get(key) + 1
                            );
                        } else {
                            System.err.println(
                                "WARNING unknown smell: " +
                                smell.getName()
                            );
                        }
                    }
                }

                out.print(csv(tc.getName()));
                out.print("," + csv(tc.getFullName()));
                out.print("," + csv(tc.getJunitVersion()));
                out.print("," + csv(tc.getProductionFile()));
                out.print("," + value(tc.getNumberLine()));
                out.print("," + value(tc.getNumberMethods()));

                for (String smell : SMELLS)
                    out.print("," + counts.get(smell));

                out.println();
            }
        }
    }

    private static String canonicalSmell(String smell) {

        if (smell == null)
            return null;

        String s = smell
            .toLowerCase()
            .replaceAll("[^a-z0-9]", "");

        return switch (s) {
            case "ar", "assertionroulette" -> "AR";
            case "ctl", "conditionaltestlogic" -> "CTL";
            case "ci", "constructorinitialization" -> "CI";
            case "dt", "defaulttest" -> "DT";
            case "dpt", "dependenttest" -> "DpT";
            case "da", "duplicateassert" -> "DA";
            case "et", "eagertest" -> "ET";
            case "emt", "emptytest" -> "EmT";
            case "ect", "exceptioncatchingthrowing" -> "ECT";
            case "gf", "generalfixture" -> "GF";
            case "igt", "ignoredtest" -> "IgT";
            case "lt", "lazytest" -> "LT";
            case "mnt", "magicnumbertest" -> "MNT";
            case "mg", "mysteryguest" -> "MG";
            case "ps", "printstatement", "redundantprint" -> "PS";
            case "ra", "redundantassertion" -> "RA";
            case "ro", "resourceoptimism" -> "RO";
            case "se", "sensitiveequality" -> "SE";
            case "st", "sleepytest" -> "ST";
            case "vt", "verbosetest" -> "VT";
            case "ut", "unknowntest" -> "UT";
            default -> null;
        };
    }

    private static String csv(Object value) {

        if (value == null)
            return "\"\"";

        String s = value.toString()
            .replace("\"", "\"\"");

        return "\"" + s + "\"";
    }

    private static String value(Object value) {
        return value == null ? "" : value.toString();
    }
}
