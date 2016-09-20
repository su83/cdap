/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React, {PropTypes} from 'react';
require('./AdminOverviewPane.less');
import OverviewPaneCard from '../OverviewPaneCard/index.js';
var shortid = require('shortid');

const propTypes = {
  isLoading: PropTypes.bool
};

function AdminOverviewPane() {

  var cards = [];
  for(var i = 0; i < 8; i++){
    cards.push(
      <OverviewPaneCard
        key={shortid.generate()}
        name="HBASE"
        version={5.6}
      />
    );
  }

  return (
    <div className="overview-pane">
      <span>Component Overview</span>
      <div className="overview-pane-container">
       {cards}
      </div>
    </div>
  );
}

AdminOverviewPane.propTypes = propTypes;

export default AdminOverviewPane;