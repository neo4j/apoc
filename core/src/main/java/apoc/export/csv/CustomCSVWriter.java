package apoc.export.csv;

import com.opencsv.CSVWriter;
import java.io.Writer;

public class CustomCSVWriter extends CSVWriter {

    private boolean shouldDifferentiateNulls;

    public CustomCSVWriter(
            Writer writer,
            char separator,
            char quoteChar,
            char escapeChar,
            String lineEnd,
            boolean shouldDifferentiateNulls) {
        super(writer, separator, quoteChar, escapeChar, lineEnd);
        this.shouldDifferentiateNulls = shouldDifferentiateNulls;
    }

    /**
     * We have overridden the openCSV writer so that empty strings are also counted as special.
     * This is so quotes will be added to them and not to null values if shouldDifferentiateNulls is true.
     */
    @Override
    protected boolean stringContainsSpecialCharacters(String line) {
        return line.indexOf(this.quotechar) != -1
                || line.indexOf(this.escapechar) != -1
                || line.indexOf(this.separator) != -1
                || line.contains("\n")
                || line.contains("\r")
                || (line.isEmpty() && shouldDifferentiateNulls);
    }
}
