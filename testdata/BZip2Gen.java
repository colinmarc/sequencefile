import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.io.IOException;

public class BZip2Gen {

    public static void gen(String path, SequenceFile.CompressionType type) throws IOException {
        Configuration config = new Configuration();
        SequenceFile.Writer writer = SequenceFile.createWriter(
                config,
                Writer.keyClass(BytesWritable.class),
                Writer.valueClass(BytesWritable.class),
                Writer.compression(type, new BZip2Codec()),
                Writer.file(new Path(path)));
        writer.append(new BytesWritable("Alice".getBytes()), new BytesWritable("Practice".getBytes()));
        writer.append(new BytesWritable("Bob".getBytes()), new BytesWritable("Hope".getBytes()));
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        gen("record_compressed_bzip2.sequencefile", SequenceFile.CompressionType.RECORD);
        gen("block_compressed_bzip2.sequencefile", SequenceFile.CompressionType.BLOCK);
    }
}
