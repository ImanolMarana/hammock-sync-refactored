/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2014 Cloudant, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.hammock.sync.internal.documentstore;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

import org.hammock.sync.documentstore.Attachment;
import org.hammock.sync.documentstore.AttachmentException;
import org.hammock.sync.documentstore.AttachmentNotSavedException;
import org.hammock.sync.documentstore.ConflictException;
import org.hammock.sync.documentstore.DocumentBodyFactory;
import org.hammock.sync.documentstore.DocumentException;
import org.hammock.sync.documentstore.DocumentRevision;
import org.hammock.sync.documentstore.UnsavedFileAttachment;
import org.hammock.sync.documentstore.UnsavedStreamAttachment;
import org.hammock.sync.documentstore.encryption.NullKeyProvider;
import org.hammock.sync.internal.sqlite.Cursor;
import org.hammock.sync.internal.sqlite.SQLCallable;
import org.hammock.sync.internal.sqlite.SQLDatabase;
import org.hammock.sync.internal.util.Misc;
import org.hammock.sync.util.TestUtils;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by tomblench on 12/03/2014.
 */
public class AttachmentTest extends BasicDatastoreTestBase {

    @Test
    public void setAndGetAttachmentsTest() throws Exception {
        String attachmentName = "attachment_1.txt";
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        DocumentRevision newRevision = null;
        try {
            DocumentRevision rev1_mut = rev_1;
            rev1_mut.getAttachments().put(attachmentName, att);
            newRevision = datastore.update(rev1_mut);
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        }
        // get attachment...
        FileInputStream fis = null;
        try {
            byte[] expectedSha1 = Misc.getSha1((fis = new FileInputStream(f)));

            SavedAttachment savedAtt = (SavedAttachment) datastore.getAttachment(newRevision.getId(),
                    newRevision.getRevision(),
                    attachmentName);
            Assert.assertArrayEquals(expectedSha1, savedAtt.key);

            SavedAttachment savedAtt2 = (SavedAttachment) datastore.attachmentsForRevision
                    ((InternalDocumentRevision)newRevision).get(attachmentName);
            Assert.assertArrayEquals(expectedSha1, savedAtt2.key);
        } catch (FileNotFoundException fnfe) {
            Assert.fail("FileNotFoundException thrown " + fnfe);
        } finally {
            IOUtils.closeQuietly(fis);
        }
    }

    // check that the transaction gets rolled back if one file is dodgy
    @Test
    public void setBadAttachmentsTest() throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        String att1Name = "attachment_1.txt";
        String att2Name = "nonexistentfile";
        String att3Name = "attachment_2.txt";
        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+att1Name), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+att2Name), "text/plain");
        Attachment att3 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+att3Name), "text/plain");
        DocumentRevision newRevision = null;
        try {
            DocumentRevision rev1_mut = rev_1;
            rev1_mut.getAttachments().put(att1Name, att1);
            rev1_mut.getAttachments().put(att2Name, att2);
            rev1_mut.getAttachments().put(att3Name, att3);
            newRevision = datastore.update(rev1_mut);
            Assert.fail("FileNotFoundException not thrown");
        } catch (AttachmentException ae) {
            // now check that things got rolled back
            datastore.runOnDbQueue(new SQLCallable<Object>() {
                @Override
                public Object call(SQLDatabase db) throws Exception {
                    Cursor c1 = db.rawQuery("select sequence from attachments;", null);
                    Assert.assertEquals("Attachments table not empty", c1.getCount(), 0);
                    Cursor c2 = db.rawQuery("select sequence from revs;", null);
                    Assert.assertEquals("Revs table not empty", c2.getCount(), 1);
                    return null;
                }
            }).get();

        }
    }

    // this test should throw a conflictexception when we try to add attachments to an old revision
    @Test(expected = DocumentException.class)
    public void setAttachmentsConflictTest() throws Exception {
        String attachmentName = "attachment_1.txt";
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);

        DocumentRevision rev_2;
        try {
            DocumentRevision rev_1_mut = rev_1;
            rev_1_mut.setBody(bodyTwo);
            rev_2 = datastore.update(rev_1_mut);
        } catch (ConflictException ce) {
            Assert.fail("ConflictException thrown: "+ce);
        }
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        DocumentRevision newRevision = rev_1;
        newRevision.getAttachments().put(attachmentName, att);
        datastore.update(newRevision);
    }

    @Test
    public void createDeleteAttachmentsTest() throws Exception{

        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        String att1Name = "attachment_1.txt";
        String att2Name = "attachment_2.txt";
        String att3Name = "bonsai-boston.jpg";
        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+att1Name), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+att2Name), "text/plain");
        Attachment att3 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+att3Name), "image/jpeg");
        DocumentRevision rev2 = null;

        DocumentRevision rev_1_mut = rev_1;
        rev_1_mut.getAttachments().put(att1Name, att1);
        rev_1_mut.getAttachments().put(att2Name, att2);
        rev_1_mut.getAttachments().put(att3Name, att3);
        rev2 = datastore.update(rev_1_mut);
        Assert.assertNotNull("Revision null", rev2);

        DocumentRevision rev3 = null;

        DocumentRevision rev2_mut = rev2;
        rev2_mut.getAttachments().remove(att1Name);
        rev3 = datastore.update(rev2_mut);
        datastore.compact();
        Assert.assertNotNull("Revision null", rev3);

        // 1st shouldn't exist
        Attachment savedAtt1 = datastore.getAttachment(rev3.getId(), rev3.getRevision(), "attachment_1.txt");
        Assert.assertNull("Att1 not null", savedAtt1);

        // check we can read from the 2nd attachment, it wasn't deleted
        Attachment savedAtt2 = datastore.getAttachment(rev3.getId(), rev3.getRevision(), "attachment_2.txt");
        Assert.assertNotNull("Att2 null", savedAtt2);
        int i2 = savedAtt2.getInputStream().read();
        Assert.assertTrue("Can't read from Att2", i2 >= 0);

        // check we can read from the 3rd attachment, it wasn't deleted
        Attachment savedAtt3 = datastore.getAttachment(rev3.getId(), rev3.getRevision(), "bonsai-boston.jpg");
        Assert.assertNotNull("Att3 null", savedAtt3);
        int i3 = savedAtt3.getInputStream().read();
        Assert.assertTrue("Can't read from Att2", i3 >= 0);

        // now sneakily look for them on disk
        File attachments = new File(datastore.datastoreDir + "/extensions/com.cloudant.attachments");
        int count = attachments.listFiles().length;
        Assert.assertEquals("Did not find 1 file in blob store", 2, count);
    }


    @Test
    public void createDeleteAttachmentsFailTest() throws Exception {
        // check that an attachment 'going missing' from the blob store doesn't stop us deleting it
        // from the database
        String attachmentName = "attachment_1.txt";
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);
        File f = TestUtils.loadFixture("fixture/"+attachmentName);
        Attachment att = new UnsavedFileAttachment(f, "text/plain");
        DocumentRevision rev2 = null;
        DocumentRevision rev_1_mut = rev_1;
        rev_1_mut.getAttachments().put(attachmentName, att);
        rev2 = datastore.update(rev_1_mut);
        Assert.assertNotNull("Revision null", rev2);

        DocumentRevision rev3 = null;
        // clear out the attachment directory
        File attachments = new File(datastore.datastoreDir + "/extensions/com.cloudant.attachments");
        for(File attFile : attachments.listFiles()) {
            attFile.delete();
        }
        DocumentRevision rev2_mut = rev2;
        rev2_mut.getAttachments().remove(attachmentName);
        rev3 = datastore.update(rev2_mut);
        Assert.assertNotNull("Revision null", rev3);

        // check that there are no attachments now associated with this doc
        Assert.assertTrue("Revision should have 0 attachments", datastore.attachmentsForRevision((InternalDocumentRevision)rev3).isEmpty());
    }


    @Test
    public void attachmentsForRevisionTest() throws Exception {
        DocumentRevision rev_1Mut = new DocumentRevision();
        rev_1Mut.setBody(bodyOne);
        DocumentRevision rev_1 = datastore.create(rev_1Mut);

        String att1Name = "attachment_1.txt";
        String att2Name = "attachment_2.txt";
        Attachment att1 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+att1Name), "text/plain");
        Attachment att2 = new UnsavedFileAttachment(TestUtils.loadFixture("fixture/"+att2Name), "text/plain");
        DocumentRevision newRevision = null;
        try {
            DocumentRevision rev_1_mut = rev_1;
            rev_1_mut.getAttachments().put(att1Name, att1);
            rev_1_mut.getAttachments().put(att2Name, att2);
            newRevision = datastore.update(rev_1_mut);
            Map<String, ? extends Attachment> attsForRev = datastore.attachmentsForRevision((InternalDocumentRevision)newRevision);
            Assert.assertEquals("Didn't get expected number of attachments", 2, attsForRev.size());
        } catch (ConflictException ce){
            Assert.fail("ConflictException thrown: "+ce);
        }
    }

    @Test
    public void duplicateAttachmentTest() throws Exception {

        DocumentRevision doc1Rev1Mut = new DocumentRevision();
        doc1Rev1Mut.setBody(bodyOne);
        DocumentRevision doc1Rev1 = datastore.create(doc1Rev1Mut);
        DocumentRevision doc2Rev1Mut = new DocumentRevision();
        doc2Rev1Mut.setBody(bodyTwo);
        DocumentRevision doc2Rev1 = datastore.create(doc2Rev1Mut);

        String attName = "attachment_1.txt";
        File attachmentFile = TestUtils.loadFixture("fixture/"+attName);
        Attachment att1 = new UnsavedFileAttachment(attachmentFile, "text/plain");
        Attachment att2 = new UnsavedFileAttachment(attachmentFile, "text/plain");

        DocumentRevision newRevisionDoc1 = null;
        DocumentRevision newRevisionDoc2 = null;

        try {
            DocumentRevision doc1Rev1_mut = doc1Rev1;
            doc1Rev1_mut.getAttachments().put(attName, att1);
            newRevisionDoc1 = datastore.update(doc1Rev1_mut);
            Assert.assertNotNull("Doc1 revision is null", newRevisionDoc1);
            Map<String, ? extends Attachment> attsForRev = datastore
                .attachmentsForRevision((InternalDocumentRevision)newRevisionDoc1);
            Assert.assertEquals("Didn't get expected number of attachments", 1,
                attsForRev.size());

            DocumentRevision doc2Rev1_mut = doc2Rev1;
            doc2Rev1_mut.getAttachments().put(attName, att2);
            newRevisionDoc2 = datastore.update(doc2Rev1_mut);
            Assert.assertNotNull("Doc2 revision is null", newRevisionDoc2);
            attsForRev = datastore.attachmentsForRevision((InternalDocumentRevision)newRevisionDoc2);
            Assert.assertEquals("Didn't get expected number of attachments", 1,
                attsForRev.size());

        } catch (ConflictException conflictException) {
            Assert.fail("Conflict Exception thrown "+conflictException);
        }
    }

    @Test
    public void testLengthPreparedAttachmentsTest() throws Exception {
        String textAttachmentName = "attachment_1.txt";
        File textFile = TestUtils.loadFixture("fixture/" + textAttachmentName);
        long expectedTextFileLength = textFile.length();

        String imageAttachmentName = "bonsai-boston.jpg";
        File imageFile = TestUtils.loadFixture("fixture/" + imageAttachmentName);
        long expectedImageFileLength = imageFile.length();

        Attachment att = new UnsavedFileAttachment(textFile, "text/plain");
        PreparedAttachment textPatt = new PreparedAttachment(att, datastore_manager_dir, 0,
                new AttachmentStreamFactory(new NullKeyProvider()));
        //Assert that the original file length is equal to the prepared attachment length
        Assert.assertEquals(expectedTextFileLength, textPatt.length);

        Attachment att2 = new UnsavedFileAttachment(imageFile, "image/jpeg");
        PreparedAttachment imagePatt = new PreparedAttachment(att2, datastore_manager_dir, 0,
                new AttachmentStreamFactory(new NullKeyProvider()));
        Assert.assertEquals(expectedImageFileLength, imagePatt.length);
    }

    @Test(expected = AttachmentNotSavedException.class)
    public void testNonexistentPreparedAttachmentsTest() throws Exception {
        String nonexistentFileName = "nonexistentfile";
        File nonExistentFile = TestUtils.loadFixture("fixture/" + nonexistentFileName);

        Attachment nonexistentAtt = new UnsavedFileAttachment(nonExistentFile, "text/plain");

        PreparedAttachment nonexistentPatt = new PreparedAttachment(nonexistentAtt, datastore_manager_dir, 0,
                new AttachmentStreamFactory(new NullKeyProvider()));
    }

    @Test
    public void testSha1PreparedAttachmentsTest() throws Exception {
        String textAttachmentName = "attachment_1.txt";
        File textFile = TestUtils.loadFixture("fixture/" + textAttachmentName);

        String imageAttachmentName = "bonsai-boston.jpg";
        File imageFile = TestUtils.loadFixture("fixture/" + imageAttachmentName);

        Attachment att = new UnsavedFileAttachment(textFile, "text/plain");
        PreparedAttachment textPatt = new PreparedAttachment(att, datastore_manager_dir, 0,
                new AttachmentStreamFactory(new NullKeyProvider()));

        byte[] textExpectedSha1 = Misc.getSha1((new FileInputStream(textFile)));
        //Assert that the expected sha1 is equal to the prepared attachment sha1
        Assert.assertArrayEquals(textExpectedSha1, textPatt.sha1);

        Attachment att2 = new UnsavedFileAttachment(imageFile, "image/jpeg");
        PreparedAttachment imagePatt = new PreparedAttachment(att2, datastore_manager_dir, 0,
                new AttachmentStreamFactory(new NullKeyProvider()));

        byte[] imageExpectedSha1 = Misc.getSha1((new FileInputStream(imageFile)));
        Assert.assertArrayEquals(imageExpectedSha1, imagePatt.sha1);

        //Assert that the text file expected sha1 is NOT equal to the prepared attachment image sha1
        Assert.assertThat(textExpectedSha1, not(equalTo(imagePatt.sha1)));
    }

    @Test
    public void testContentPreparedAttachmentsTest() throws Exception {
        String imageAttachmentName = "bonsai-boston.jpg";
        File imageFile = TestUtils.loadFixture("fixture/" + imageAttachmentName);

        Attachment att2 = new UnsavedFileAttachment(imageFile, "image/jpeg");
        PreparedAttachment imagePatt = new PreparedAttachment(att2, datastore_manager_dir, 0,
                new AttachmentStreamFactory(new NullKeyProvider()));

        IOUtils.contentEquals(
                new FileInputStream(imageFile),
                new FileInputStream(imagePatt.tempFile));
    }

    @Test
    public void attachmentOrderingTest() throws Exception {
        DocumentRevision doc = new DocumentRevision();
        doc.setBody(bodyOne);

        int nAtts = 100;
        List<Integer> numbers = new ArrayList<Integer>();
        for (int i=0; i<nAtts; i++) {
            numbers.add(i);
        }
        Collections.shuffle(numbers);
        for (int i : numbers) {
            String name = "attachment_" + i;
            StringBuilder s = new StringBuilder();
            s.append("this is some data for ");
            s.append(name);
            byte[] bytes = (s.toString()).getBytes();
            Attachment att0 = new UnsavedStreamAttachment(new ByteArrayInputStream(bytes), "text/plain");
            doc.getAttachments().put(name, att0);
        }
        doc = datastore.create(doc);
        List<InternalDocumentRevision> path = new ArrayList<InternalDocumentRevision>();
        path.add((InternalDocumentRevision)doc);
        boolean shouldInline = false;
        int minRevPos = 0;
        Map<String, Object> json = RevisionHistoryHelper.revisionHistoryToJson(path,
                doc.getAttachments(),
                shouldInline,
                minRevPos);
        MultipartAttachmentWriter mpw = RevisionHistoryHelper.createMultipartWriter(json,
                doc.getAttachments(),
                shouldInline,
                minRevPos);
        File f = TestUtils.loadFixture("fixture/multipart_100_atts_ordered.regex");
        Pattern p = Pattern.compile(IOUtils.toString(new FileInputStream(f)), Pattern.MULTILINE | Pattern.DOTALL);
        String regex = new String(IOUtils.toByteArray(mpw.makeInputStream()), Charset.forName("UTF-8"));
        Matcher m = p.matcher(regex);
        Assert.assertTrue("Should match pattern", m.matches());
    }

    // test for regression on fetching attachments after deleting revision which previously had no
    // attachments
    @Test
    public void getAttachmentsOnDeletedRevisionNoAttachments() throws Exception {

        DocumentRevision revision = new DocumentRevision();
        Map<String, Object> body = new HashMap<String, Object>();
        body.put("animal", "cat");
        revision.setBody(DocumentBodyFactory.create(body));

        // create rev
        DocumentRevision saved = documentStore.database().create(revision);
        Map<String, Attachment> attachments = saved.getAttachments();
        Assert.assertTrue(attachments.isEmpty());

        DocumentRevision deleted = documentStore.database().delete(saved);
        attachments = deleted.getAttachments();

        Assert.assertTrue(attachments.isEmpty());
    }


    // test for regression on fetching attachments after deleting revision which previously had some
    // attachments
    @Test
    public void getAttachmentsOnDeletedRevisionSomeAttachments() throws Exception {

        DocumentRevision revision = new DocumentRevision();
        Map<String, Object> body = new HashMap<String, Object>();
        body.put("animal", "cat");
        revision.setBody(DocumentBodyFactory.create(body));

        // add attachment
        String imageAttachmentName = "bonsai-boston.jpg";
        File imageFile = TestUtils.loadFixture("fixture/" + imageAttachmentName);
        Attachment att = new UnsavedFileAttachment(imageFile, "image/jpeg");
        revision.getAttachments().put(imageAttachmentName, att);

        //create rev
        DocumentRevision saved = documentStore.database().create(revision);
        Map<String, Attachment> attachments = saved.getAttachments();
        Assert.assertEquals(attachments.size(), 1);

        DocumentRevision deleted = documentStore.database().delete(saved);
        attachments = deleted.getAttachments();

        Assert.assertTrue(attachments.isEmpty());
    }

}
