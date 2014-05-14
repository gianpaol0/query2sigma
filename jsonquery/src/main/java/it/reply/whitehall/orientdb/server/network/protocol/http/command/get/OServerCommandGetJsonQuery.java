package it.reply.whitehall.orientdb.server.network.protocol.http.command.get;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
import com.tinkerpop.blueprints.impls.orient.OrientElement;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;

public class OServerCommandGetJsonQuery extends
		OServerCommandAuthenticatedDbAbstract {
	private static final String[] NAMES = { "GET|jsonquery/*" };
	
	
	public OServerCommandGetJsonQuery() {
	  }

	  public OServerCommandGetJsonQuery(final OServerCommandConfiguration iConfig) {
	  }
	
	@Override
	@SuppressWarnings("unchecked")
	public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse)
			throws Exception {

		String[] urlParts = checkSyntax(
				iRequest.url,
				4,
				"Syntax error: jsonquery/<database>/<language>/<query-text>[/<limit>][/<fetchPlan>/edgeType].<br/>Limit is optional and is setted to 20 by default. Set expressely to 0 to have no limits.");

		final String language = urlParts[2];
		final String text = urlParts[3];
		final int limit = urlParts.length > 4 ? Integer.parseInt(urlParts[4])
				: 20;
		final String fetchPlan = urlParts.length > 5 ? urlParts[5] : null;
		final String edgeType = urlParts.length > 6 ? urlParts[6] : null;
		
		iRequest.data.commandInfo = "Gexf";
		iRequest.data.commandDetail = text;

		final ODatabaseDocumentTx db = getProfiledDatabaseInstance(iRequest);

		final OrientGraph graph = OGraphCommandExecutorSQLFactory
				.getGraph(false);
		try {

			final Iterable<OrientElement> vertices;
			if (language.equals("sql"))
				vertices = graph.command(
						new OSQLSynchQuery<OrientElement>(text, limit)
								.setFetchPlan(fetchPlan)).execute();
			else if (language.equals("gremlin")) {
				List<Object> result = new ArrayList<Object>();
				OGremlinHelper.execute(graph, text, null, null, result, null,
						null);

				vertices = new ArrayList<OrientElement>(result.size());

				for (Object o : result) {
					((ArrayList<OrientElement>) vertices).add((OrientElement)((OIdentifiable)o).getRecord());
				}
			} else
				throw new IllegalArgumentException("Language '" + language
						+ "' is not supported. Use 'sql' or 'gremlin'");

			sendRecordsContent(iRequest, iResponse, vertices, fetchPlan,edgeType);

		} finally {
			if (graph != null)
				graph.shutdown();

			if (db != null)
				db.close();
		}

		return false;
	}

	protected void sendRecordsContent(final OHttpRequest iRequest,
			final OHttpResponse iResponse, Iterable<OrientElement> iRecords,
			String iFetchPlan, String edgeType) throws IOException {
		final StringWriter buffer = new StringWriter();
		final OJSONWriter json= new OJSONWriter(buffer, OHttpResponse.JSON_FORMAT);
		json.setPrettyPrint(true);
		 
		generateGraphDbOutput(iRecords, json, edgeType);

		iResponse.send(OHttpUtils.STATUS_OK_CODE,
				OHttpUtils.STATUS_OK_DESCRIPTION,OHttpResponse.JSON_FORMAT,
				buffer.toString(), null);
	}

	protected void generateGraphDbOutput(
			final Iterable<OrientElement> iVertices, final OJSONWriter json, String edgeType) throws IOException {
		if (iVertices == null)
			return;
		
		final Set<OrientVertex> nodes=new HashSet<OrientVertex>();
		final Set<OrientEdge> edges=new HashSet<OrientEdge>();
		
		
		//Suddivido tra vertici ed edges
		for(OrientElement o : iVertices){
			
			if(o instanceof OrientVertex){
				
				nodes.add((OrientVertex)o);
				
			}
			else if(o instanceof OrientEdge){
				
				edges.add((OrientEdge) o);
			}
			
		}
		json.beginObject(0, false, null);
		
		if(nodes.size()>0)
		{
			json.resetAttributes();
			
			json.beginCollection(0,true,"nodes");
			 int i=0;
			 for (OrientVertex vertex : nodes) {
				  i++;
			      json.resetAttributes();
			      json.beginObject(1, false, null);
			      json.writeAttribute(2,false, "id", vertex.getIdentity());

			      // ADD ALL THE PROPERTIES
			      for (String field : vertex.getPropertyKeys()) {
			        final Object v = vertex.getProperty(field);
			        if (v != null)
			          json.writeAttribute(2, false, field, v);
			      }
			      json.endObject(1, false);
			      if(i!=nodes.size())
			    	  json.append(",");
			      json.newline();
			    }

			json.endCollection();
			json.newline();
			
		}
		
		if(edges.size()>0){
	
			json.beginCollection(0,true,"edges");
			int i=0;
			for (OrientEdge edge : edges) {
					
				  i++;
			      json.resetAttributes();
			      json.beginObject(1, false, null);
			      
			      if(edgeType!=null && !"".equals(edgeType))
			    	  json.writeAttribute(2, false, "directed", edgeType);

			      json.writeAttribute(2, false, "source", edge.getVertex(Direction.OUT).getId());
			      json.writeAttribute(2, false, "target", edge.getVertex(Direction.IN).getId());
			      
			      for (String field : edge.getPropertyKeys()) {
			        final Object v = edge.getProperty(field);
			        if (v != null)
			          json.writeAttribute(2, false, field, v);
			      }

			      json.endObject(1, false);
			      if(i!=edges.size())
			    	  json.append(",");
			      json.newline();
			    }
			json.endCollection();
		}
		
		json.endObject(0);
			
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
	

}