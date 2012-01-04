package be.datablend.spatial.running;

import com.vividsolutions.jts.geom.Coordinate;
import net.sourceforge.gpstools.GPSDings;
import net.sourceforge.gpstools.TrackAnalyzer;
import net.sourceforge.gpstools.gpx.Gpx;
import net.sourceforge.gpstools.gpx.Trkseg;
import org.apache.commons.math.FunctionEvaluationException;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.EditableLayerImpl;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.SimplePointEncoder;
import org.neo4j.gis.spatial.pipes.GeoPipeFlow;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

/**
 * User: dsuvee
 * Date: 04/01/12
 */
public class ImportData {

    private EmbeddedGraphDatabase graphDb = null;
    private SpatialDatabaseService spatialDb = null;
    private EditableLayer runningLayer = null;

    private static enum RelTypes implements RelationshipType { NEXT }

    public ImportData() {
        // Create the graph db
        graphDb = new EmbeddedGraphDatabase("var/geo");
        // Wrap it as a spatial db service
        spatialDb = new SpatialDatabaseService(graphDb);
        // Create the layer to store our spatial data
        runningLayer = (EditableLayer) spatialDb.getOrCreateLayer("running", SimplePointEncoder.class, EditableLayerImpl.class, "lon:lat");
    }

    // Import the data from a GPX file. Boolean indicates whether data has been imported before
    public void addData(File file, boolean firsttime) throws IOException, FunctionEvaluationException {

        // Start by reading the file and analyzing it contents
        Gpx gpx = GPSDings.readGPX(new FileInputStream(file));
        TrackAnalyzer analyzer = new TrackAnalyzer();
        analyzer.addAllTracks(gpx);
        // The garmin GPX running data contains only one track containing one segment
        Trkseg track = gpx.getTrk(0).getTrkseg(0);

        // Start a new transaction
        Transaction tx = graphDb.beginTx();
        // Contains the record that was added previously (in order to create a relation between the new and the previous node)
        SpatialDatabaseRecord fromrecord = null;

        // Iterate all points
        for (int i = 0; i < track.getTrkptCount(); i++) {

            // Create a new coordinate for this point
            Coordinate to = new Coordinate(track.getTrkpt(i).getLon().doubleValue(),track.getTrkpt(i).getLat().doubleValue());

            // Check whether we can find a node from which is located within a distance of 20 meters
            List<GeoPipeFlow> closests = GeoPipeline.startNearestNeighborLatLonSearch(runningLayer, to, 0.02).sort("OrthodromicDistance").getMin("OrthodromicDistance").toList();
            SpatialDatabaseRecord torecord = null;

            // If first time, we add all nodes. Otherwise, we check whether we find a node that is close enough to the current location
            if (!firsttime && (closests.size() == 1)) {
                // Retrieve the node
                System.out.println("Using existing: " + closests.get(0).getProperty("OrthodromicDistance"));
                torecord = closests.get(0).getRecord();
                // Recalculate average speed
                double previousspeed  =  (Double)torecord.getProperty("speed");
                int previousoccurences =  (Integer)torecord.getProperty("occurences");
                double currentspeed = analyzer.getHorizontalSpeed(track.getTrkpt(i).getTime());
                double denormalizespeed = previousspeed * previousoccurences;
                double newspeed = ((denormalizespeed + currentspeed) / (previousoccurences + 1));
                // Update the data accordingly
                torecord.setProperty("speed",newspeed);
                torecord.setProperty("occurences",previousoccurences+1);
            }
            else {
                // New node, add it
                torecord = runningLayer.add(runningLayer.getGeometryFactory().createPoint(to));
                // Set the data accordingly
                torecord.setProperty("speed", analyzer.getHorizontalSpeed(track.getTrkpt(i).getTime()));
                torecord.setProperty("occurences", 1);
            }

            // If a previous node is available (and they are not identical), add a directed relationship between both
            if (fromrecord != null && (!fromrecord.equals(torecord)))  {
                Relationship next = fromrecord.getGeomNode().createRelationshipTo(torecord.getGeomNode(), RelTypes.NEXT);
            }
            // Previous record is put on new record
            fromrecord = torecord;
        }

        // Commit transaction
        tx.success();
        tx.finish();

    }

    public static void main(String[] args) throws IOException, FunctionEvaluationException {
        // Retrieve all GPX files to import
        File path = new File(ImportData.class.getClassLoader().getResource("runs").getFile());
        File[] files = path.listFiles();

        // Import the data in Neo4J Spatial datastore
        ImportData importer = new ImportData();
        // Add the first one, detailing that we have not imported data yet
        importer.addData(files[0], true);
        // Add the rest
        for (int i = 1; i < files.length; i++) {
            importer.addData(files[i], false);
        }

    }


}
