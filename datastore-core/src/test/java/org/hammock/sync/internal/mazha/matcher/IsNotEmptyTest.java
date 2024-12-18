/*
 * Copyright © 2017 IBM Corp. All rights reserved.
 *
 * Copyright © 2013 Cloudant, Inc. All rights reserved.
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

package org.hammock.sync.internal.mazha.matcher;

import org.junit.Assert;
import org.junit.Test;

import static org.hammock.sync.internal.mazha.matcher.IsNotEmpty.notEmpty;
import static org.hamcrest.CoreMatchers.is;

public class IsNotEmptyTest {

    @Test
    public void notEmpty_nonEmptyString() {
        Assert.assertThat("a", is(notEmpty()));
    }

    @Test(expected = AssertionError.class)
    public void notEmpty_emptyString() {
        Assert.assertThat("", is(notEmpty()));
    }
}
