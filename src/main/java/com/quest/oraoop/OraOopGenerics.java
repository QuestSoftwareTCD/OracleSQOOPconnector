/**
 *   Copyright 2011 Quest Software, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.quest.oraoop;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class OraOopGenerics {

    public class ListRandomizer<T> {

        public void randomizeList(List<T> list) {

            if (list == null)
                throw new IllegalArgumentException("No list was passed.");

            if (list.size() == 0)
                return;

            Random random = new Random();
            int randomIdx;
            T tmp;

            // The "repeats" loop performs the "randomization of the list"
            // multiple times. If the "randomization of the list" is only
            // performed once, the results is a noticeably non-uniform
            // distribution of combinations. i.e. In a list containing 4
            // items, 4 (of the 24 possible) combinations routinely occur
            // 30% more frequently than the other (20 possible) combinations.
            // This is quite obvious when a histogram of the combinations
            // is plotted.
            // (Change the upper-bound of the repeats loop to 2 and then run
            // the "testRandomizeList" unit-test to observe this.)
            // In practice, it seems we only need to repeat the
            // "randomization of the list" twice in order for the histogram
            // of combinations to become much more uniform.
            for (int repeats = 0; repeats < 2; repeats++) {
                for (int idx = 0; idx < list.size(); idx++) {
                    randomIdx = random.nextInt(list.size());
                    if (idx != randomIdx) {
                        tmp = list.get(idx);
                        list.set(idx, list.get(randomIdx));
                        list.set(randomIdx, tmp);
                    }
                }
            }

        }
    }

    public static class ObjectList<T> {

        private List<T> objects;

        public ObjectList() {

            this.objects = new ArrayList<T>();
        }

        public void add(T item) {

            this.objects.add(item);
        }

        public int size() {

            return this.objects.size();
        }

        public T get(int index) {

            return this.objects.get(index);
        }

        public Iterator<T> iterator() {

            return this.objects.iterator();
        }

    }

}
