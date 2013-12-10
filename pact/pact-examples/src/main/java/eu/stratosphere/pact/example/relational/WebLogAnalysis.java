/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.example.relational;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.CoGroupStub;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MapStub;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.common.util.FieldSet;

/**
 * Implements the following relational OLAP query as PACT program:
 * 
 * <code><pre>
 * SELECT r.pageURL, r.pageRank, r.avgDuration
 * FROM Documents d JOIN Rankings r
 * 	ON d.url = r.url
 * WHERE CONTAINS(d.text, [keywords])
 * 	AND r.rank > [rank]
 * 	AND NOT EXISTS (
 * 		SELECT * FROM Visits v
 * 		WHERE v.destUrl = d.url
 * 			AND v.visitDate < [date]); 
 *  * </pre></code> 
 * 
 * Table Schemas: <code><pre>
 * CREATE TABLE Documents (
 * 					url VARCHAR(100) PRIMARY KEY,
 * 					contents TEXT );
 * 
 * CREATE TABLE Rankings (
 * 					pageRank INT,
 * 					pageURL VARCHAR(100) PRIMARY KEY,     
 * 					avgDuration INT );       
 * 
 * CREATE TABLE Visits (
 * 					sourceIP VARCHAR(16),
 * 					destURL VARCHAR(100),
 * 					visitDate DATE,
 * 					adRevenue FLOAT,
 * 					userAgent VARCHAR(64),
 * 					countryCode VARCHAR(3),
 * 					languageCode VARCHAR(6),
 * 					searchWord VARCHAR(32),
 * 					duration INT );
 * </pre></code>
 * 
 * @author Fabian Hueske
 */
public class WebLogAnalysis implements PlanAssembler, PlanAssemblerDescription
{
	
	/**
	 * MapStub that filters for documents that contain a certain set of
	 * keywords. 
	 */
	@ConstantFieldsExcept(1)
	public static class FilterDocs extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private static final String[] KEYWORDS = { " editors ", " oscillations ", " convection " };
		
		/**
		 * Filters for documents that contain all of the given keywords and projects the records on the URL field.
		 * 
		 * Output Format:
		 * 0: URL
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			// FILTER
			// Only collect the document if all keywords are contained
			String docText = record.getField(1, PactString.class).toString();
			boolean allContained = true;
			for (String kw : KEYWORDS) {
				if (!docText.contains(kw)) {
					allContained = false;
					break;
				}
			}

			if (allContained) {
				record.setNull(1);
				out.collect(record);
			}
		}
	}

	/**
	 * MapStub that filters for records where the rank exceeds a certain threshold.
	 */
	@ConstantFieldsExcept({})
	public static class FilterRanks extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private static final int RANKFILTER = 50;
		
		/**
		 * Filters for records of the rank relation where the rank is greater
		 * than the given threshold.
		 * 
		 * Output Format:
		 * 0: URL
		 * 1: RANK
		 * 2: AVG_DURATION
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			
			if (record.getField(1, PactInteger.class).getValue() > RANKFILTER) {
				out.collect(record);
			}
		}
	}

	/**
	 * MapStub that filters for records of the visits relation where the year
	 * (from the date string) is equal to a certain value.
	 */
	@ConstantFieldsExcept(1)
	public static class FilterVisits extends MapStub implements Serializable {
		private static final long serialVersionUID = 1L;

		private static final int YEARFILTER = 2010;
		
		/**
		 * Filters for records of the visits relation where the year of visit is equal to a
		 * specified value. The URL of all visit records passing the filter is emitted.
		 * 
		 * Output Format:
		 * 0: URL
		 */
		@Override
		public void map(PactRecord record, Collector<PactRecord> out) throws Exception {
			// Parse date string with the format YYYY-MM-DD and extract the year
			String dateString = record.getField(1, PactString.class).getValue();
			int year = Integer.parseInt(dateString.substring(0,4)); 
			
			if (year == YEARFILTER) {
				record.setNull(1);
				out.collect(record);
				
			}
		}
	}

	/**
	 * MatchStub that joins the filtered entries from the documents and the
	 * ranks relation.
	 */
	@ConstantFieldsSecondExcept({})
	public static class JoinDocRanks extends MatchStub implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * Joins entries from the documents and ranks relation on their URL.
		 * 
		 * Output Format:
		 * 0: URL
		 * 1: RANK
		 * 2: AVG_DURATION
		 */
		@Override
		public void match(PactRecord document, PactRecord rank, Collector<PactRecord> out) throws Exception {
			out.collect(rank);	
		}
	}

	/**
	 * CoGroupStub that realizes an anti-join.
	 * If the first input does not provide any pairs, all pairs of the second input are emitted.
	 * Otherwise, no pair is emitted.
	 */
	@ConstantFieldsFirstExcept({})
	public static class AntiJoinVisits extends CoGroupStub implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * If the visit iterator is empty, all pairs of the rank iterator are emitted.
		 * Otherwise, no pair is emitted. 
		 * 
		 * Output Format:
		 * 0: URL
		 * 1: RANK
		 * 2: AVG_DURATION
		 */
		@Override
		public void coGroup(Iterator<PactRecord> ranks, Iterator<PactRecord> visits, Collector<PactRecord> out) {
			// Check if there is a entry in the visits relation
			if (!visits.hasNext()) {
				while (ranks.hasNext()) {
					// Emit all rank pairs
					out.collect(ranks.next());
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {

		// parse job parameters
		int numSubTasks     = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String docsInput   = (args.length > 1 ? args[1] : "");
		String ranksInput  = (args.length > 2 ? args[2] : "");
		String visitsInput = (args.length > 3 ? args[3] : "");
		String output      = (args.length > 4 ? args[4] : "");

		/*
		 * Output Format:
		 * 0: URL
		 * 1: DOCUMENT_TEXT
		 */
		// Create DataSourceContract for documents relation
		@SuppressWarnings("unchecked")
		RecordInputFormat docsFormat = new RecordInputFormat('|', PactString.class, PactString.class);
		FileDataSource docs = new FileDataSource(docsFormat, docsInput, "Docs Input");
		
		/*
		 * Output Format:
		 * 0: URL
		 * 1: RANK
		 * 2: AVG_DURATION
		 */
		// Create DataSourceContract for ranks relation
		FileDataSource ranks = new FileDataSource(new RecordInputFormat(), ranksInput, "Ranks input");
		RecordInputFormat.configureRecordFormat(ranks)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.field(PactString.class, 1)
			.field(PactInteger.class, 0)
			.field(PactInteger.class, 2);

		/*
		 * Output Format:
		 * 0: URL
		 * 1: DATE
		 */
		// Create DataSourceContract for visits relation
		@SuppressWarnings("unchecked")
		RecordInputFormat visitsFormat = new RecordInputFormat('|', null, PactString.class, PactString.class);
		FileDataSource visits = new FileDataSource(visitsFormat, visitsInput, "Visits input:q");

		// Create MapContract for filtering the entries from the documents
		// relation
		MapContract filterDocs = MapContract.builder(new FilterDocs())
			.input(docs)
			.name("Filter Docs")
			.build();
		filterDocs.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.15f);
		filterDocs.getCompilerHints().setAvgBytesPerRecord(60);
		filterDocs.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0}), 1);

		// Create MapContract for filtering the entries from the ranks relation
		MapContract filterRanks = MapContract.builder(new FilterRanks())
			.input(ranks)
			.name("Filter Ranks")
			.build();
		filterRanks.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.25f);
		filterRanks.getCompilerHints().setAvgNumRecordsPerDistinctFields(new FieldSet(new int[]{0}), 1);

		// Create MapContract for filtering the entries from the visits relation
		MapContract filterVisits = MapContract.builder(new FilterVisits())
			.input(visits)
			.name("Filter Visits")
			.build();
		filterVisits.getCompilerHints().setAvgBytesPerRecord(60);
		filterVisits.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.2f);

		// Create MatchContract to join the filtered documents and ranks
		// relation
		MatchContract joinDocsRanks = MatchContract.builder(new JoinDocRanks(), PactString.class, 0, 0)
			.input1(filterDocs)
			.input2(filterRanks)
			.name("Join Docs Ranks")
			.build();
		joinDocsRanks.setDegreeOfParallelism(numSubTasks);

		// Create CoGroupContract to realize a anti join between the joined
		// documents and ranks relation and the filtered visits relation
		CoGroupContract antiJoinVisits = CoGroupContract.builder(new AntiJoinVisits(), PactString.class, 0, 0)
			.input1(joinDocsRanks)
			.input2(filterVisits)
			.name("Antijoin DocsVisits")
			.build();
		antiJoinVisits.getCompilerHints().setAvgRecordsEmittedPerStubCall(0.8f);

		// Create DataSinkContract for writing the result of the OLAP query
		FileDataSink result = new FileDataSink(new RecordOutputFormat(), output, antiJoinVisits, "Result");
		result.setDegreeOfParallelism(numSubTasks);
		RecordOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter('|')
			.lenient(true)
			.field(PactInteger.class, 1)
			.field(PactString.class, 0)
			.field(PactInteger.class, 2);

		// Return the PACT plan
		Plan p = new Plan(result, "Weblog Analysis");
		p.setDefaultParallelism(numSubTasks);
		return p;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [numSubTasks], [docs], [ranks], [visits], [output]";
	}
}
