
internal class TestSeqs : MongoDbTest {
	
	Void testSeqs() {
		id	 := 0
		seqs := MongoSeqs(db["Seqs"])
		
		id	= seqs.nextId("Seq1")
		verifyEq(id, 1)
		id	= seqs.nextId("Seq1")
		verifyEq(id, 2)
		id	= seqs.nextId("Seq1")
		verifyEq(id, 3)

		id	= seqs.nextId("Seq2")
		verifyEq(id, 1)
		id	= seqs.nextId("Seq2")
		verifyEq(id, 2)
		id	= seqs.nextId("Seq2")
		verifyEq(id, 3)
		
		id	= seqs.nextId("Seq1")
		verifyEq(id, 4)
		id	= seqs.nextId("Seq2")
		verifyEq(id, 4)
		
		seqs.reset("Seq1")

		id	= seqs.nextId("Seq1")
		verifyEq(id, 1)
		id	= seqs.nextId("Seq2")
		verifyEq(id, 5)
		
		seqs.resetAll
		
		id	= seqs.nextId("Seq1")
		verifyEq(id, 1)
		id	= seqs.nextId("Seq2")
		verifyEq(id, 1)
		
		verifyEq(seqs.seqColl.size, 2)
	}
}
