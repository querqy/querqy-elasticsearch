package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.Version;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.DataOutputStreamOutput;
import org.elasticsearch.common.transport.TransportAddress;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

public class NodesReloadRewriterResponseTest {

    @Test
    public void testWriteToReadFromStream() throws IOException {

        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final DataOutputStreamOutput dos = new DataOutputStreamOutput(new DataOutputStream(bos));

        NullPointerException npe = new NullPointerException("test");
        npe.fillInStackTrace();
        NodesReloadRewriterResponse response = new NodesReloadRewriterResponse(
                        new ClusterName("c1"),
                        Arrays.asList(
                                new NodesReloadRewriterResponse.NodeResponse(
                                    new DiscoveryNode("n1",
                                            new TransportAddress(TransportAddress.META_ADDRESS, 9234),
                                            Version.CURRENT), npe),
                                new NodesReloadRewriterResponse.NodeResponse(
                                        new DiscoveryNode("n2",
                                                new TransportAddress(TransportAddress.META_ADDRESS, 9235),
                                                Version.CURRENT), null)

                        ), Collections.singletonList(new FailedNodeException("n3", "node 3 down",
                new SocketException())));

        response.writeTo(dos);
        dos.flush();
        dos.close();

        final ByteBufferStreamInput byteInput = new ByteBufferStreamInput(ByteBuffer.wrap(bos.toByteArray()));
        final NodesReloadRewriterResponse response1 = new NodesReloadRewriterResponse(byteInput);
        assertEquals(response, response1);


    }

}