#include "Join.hpp"
#include <functional>

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */
vector<Bucket> partition(
    Disk* disk, 
    Mem* mem, 
    pair<unsigned int, unsigned int> left_rel, 
    pair<unsigned int, unsigned int> right_rel) {
    
    //0: we need B-1 partition/buckets in the disk
    //in memory, we use 1 partition for input, other B-1 for hashsort
    vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk));
    
    //1: partition on right relation
    for (unsigned int i = right_rel.first; i < right_rel.second; i++) {
        //2: load each disk page into memory last page (index B-1)
        mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
        //3: get the input page pointer in the memory
        Page* pg = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
        //4: look through all records in it and sort it
        for (unsigned int j = 0; j < pg->size(); j++) {
            //5: get the final parge idx for this record
            Record re = pg->get_record(j);
            unsigned int idx = re.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            Page* finalPage = mem->mem_page(idx);
            //6: check whether the finalPage is full
            if (finalPage->size() >= RECORDS_PER_PAGE) {
                //7: push final page in mem into disk and reset it
                partitions[idx].add_right_rel_page(mem->flushToDisk(disk, idx));
            }
            //8: then push record into this final page
            finalPage->loadRecord(re);
        }
    }
    
    //9: after the iter, push remained page with records into disk
    for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
        //10: only consider not-empty page in mem
        if (mem->mem_page(i)->size() > 0) {
            partitions[i].add_right_rel_page(mem->flushToDisk(disk, i));
        }
    }
    //11: we need to reset the memory
    mem->reset();
    
    //12: do the same thing with left relation
    for (unsigned int i = left_rel.first; i < left_rel.second; i++) {
        //13: load each disk page into memory last page (index B-1)
        mem->loadFromDisk(disk, i, MEM_SIZE_IN_PAGE - 1);
        //14: get the input page pointer in the memory
        Page* pg = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
        //15: look through all records in it and sort it
        for (unsigned int j = 0; j < pg->size(); j++) {
            //16: get the final parge idx for this record
            Record re = pg->get_record(j);
            unsigned int idx = re.partition_hash() % (MEM_SIZE_IN_PAGE - 1);
            Page* finalPage = mem->mem_page(idx);
            //17: check whether the finalPage is full
            if (finalPage->size() >= RECORDS_PER_PAGE) {
                //18: push final page in mem into disk and reset it
                partitions[idx].add_left_rel_page(mem->flushToDisk(disk, idx));
            }
            //19: then push record into this final page
            finalPage->loadRecord(re);
        }
    }
    
    //20: after the iter, push remained page with records into disk
    for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 1; i++) {
        //21: only consider not-empty page in mem
        if (mem->mem_page(i)->size() > 0) {
            partitions[i].add_left_rel_page(mem->flushToDisk(disk, i));
        }
    }
    //22: we need to reset the memory
    mem->reset();
    
    return partitions;
}

/*
 * TODO: Student implementation
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<unsigned int> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
    //1: we need to reset the memory
    mem->reset();
    //2: we need a DS to store the results
    vector<unsigned int> joinResult;
    
    //3: we should look through all partitions
    //we use B-2 new partition region and 1 for input, 1 for output
    for (unsigned int i = 0; i < partitions.size(); i++) {
        //4: for each parititon, if the right relation is smaller one
        if (partitions[i].num_right_rel_record < partitions[i].num_left_rel_record) {
            //5: get a list of disk page ids for right relation
            vector<unsigned int> rightAllPages = partitions[i].get_right_rel();
            //6: for each right page in this partition, push it into mem and hash2 it
            for (unsigned int j = 0; j < rightAllPages.size(); j++) {
                //7: load each page into the mem
                mem->loadFromDisk(disk, rightAllPages[j], MEM_SIZE_IN_PAGE - 1);
                Page* rPage = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                //8: hash2 each records in this page (input page position)
                for (unsigned int k = 0; k < rPage->size(); k++) {
                    //9: get hash2 idx
                    Record re = rPage->get_record(k);
                    unsigned int newIdx = re.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                    //10: push record into new hash2 page
                    mem->mem_page(newIdx)->loadRecord(re);
                }
            }
            //7: finish the right part, begin probing on the left part
            vector<unsigned int> leftAllPages = partitions[i].get_left_rel();
            //8: look through all pages in this partition, push each into mem and probe it
            for (unsigned int j = 0; j < leftAllPages.size(); j++) {
                //9: load each page into the mem (input page position)
                mem->loadFromDisk(disk, leftAllPages[j], MEM_SIZE_IN_PAGE - 1);
                Page* lPage = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                //10: look through each record in this page
                for (unsigned int k = 0; k < lPage->size(); k++) {
                    Record reL = lPage->get_record(k);
                    unsigned int newIdx = reL.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                    //11: locate this hash2 page and probe it
                    Page* matchPage = mem->mem_page(newIdx);
                    for (unsigned int m = 0; m < matchPage->size(); m++) {
                        Record match = matchPage->get_record(m);
                        //12: find the match records
                        if (reL == match) {
                            //13: push the pair into output Page in mem (index B-2)
                            //14: but we need to first check whether it is full
                            if (mem->mem_page(MEM_SIZE_IN_PAGE - 2)->size() >= RECORDS_PER_PAGE) {
                                //flushToDisk return the disk page id
                                joinResult.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 2));
                            }
                            //15: load the pair into output page
                            mem->mem_page(MEM_SIZE_IN_PAGE - 2)->loadPair(reL, match);
                        }
                    }
                }
            }
        //16: do the all things when left relation is smaller
        } else {
            //17: get a list of disk page ids for left relation
            vector<unsigned int> leftAllPages = partitions[i].get_left_rel();
            //18: for each left page in this partition, push it into mem and hash2 it
            for (unsigned int j = 0; j < leftAllPages.size(); j++) {
                //19: load each page into the mem
                mem->loadFromDisk(disk, leftAllPages[j], MEM_SIZE_IN_PAGE - 1);
                Page* lPage = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                //20: hash2 each records in this page (input page position)
                for (unsigned int k = 0; k < lPage->size(); k++) {
                    //21: get hash2 idx
                    Record reL = lPage->get_record(k);
                    unsigned int newIdx = reL.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                    //22: push record into new hash2 page
                    mem->mem_page(newIdx)->loadRecord(reL);
                }
            }
            //23: finish the left part, begin probing on the right part
            vector<unsigned int> rightAllPages = partitions[i].get_right_rel();
            //24: look through all pages in this partition, push each into mem and probe it
            for (unsigned int j = 0; j < rightAllPages.size(); j++) {
                //25: load each page into the mem (input page position)
                mem->loadFromDisk(disk, rightAllPages[j], MEM_SIZE_IN_PAGE - 1);
                Page* rPage = mem->mem_page(MEM_SIZE_IN_PAGE - 1);
                //26: look through each record in this page
                for (unsigned int k = 0; k < rPage->size(); k++) {
                    Record re = rPage->get_record(k);
                    unsigned int newIdx = re.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
                    //27: locate this hash2 page and probe it
                    Page* matchPage = mem->mem_page(newIdx);
                    for (unsigned int m = 0; m < matchPage->size(); m++) {
                        //28: find the match records
                        Record match = matchPage->get_record(m);
                        if (re == match) {
                            //29: push the pair into output Page in mem (index B-2)
                            //30: but we need to first check whether it is full
                            if (mem->mem_page(MEM_SIZE_IN_PAGE - 2)->size() >= RECORDS_PER_PAGE) {
                                //flushToDisk return the disk page id
                                joinResult.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 2));
                            }
                            //31: load the pair into output page
                            mem->mem_page(MEM_SIZE_IN_PAGE - 2)->loadPair(match, re);
                        }
                    }
                }
            }
        }
        
        //32: we need to reset the memory
        for (unsigned int i = 0; i < MEM_SIZE_IN_PAGE - 2; i++) {
            mem->mem_page(i)->reset();
        }
    }
    //33: if output page remains some records and is not full, just push it into the disk
    if (mem->mem_page(MEM_SIZE_IN_PAGE - 2)->size() > 0) {
        joinResult.push_back(mem->flushToDisk(disk, MEM_SIZE_IN_PAGE - 2));
    }
    
    return joinResult;
}

