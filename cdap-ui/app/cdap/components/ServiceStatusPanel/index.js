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

import React, {Component, PropTypes} from 'react';
require('./ServiceStatusPanel.less');
import ServiceStatus from '../ServiceStatus/index.js';
import {MyServiceProviderApi} from 'api/serviceproviders';
var shortid = require('shortid');

const propTypes = {
  services: PropTypes.array,
  isLoading: PropTypes.bool
};

export default class ServiceStatusPanel extends Component {
  constructor(props){
    super(props);
    MyServiceProviderApi.get()
      .subscribe(
        (res) => {
          console.log('res: ', res);
        }
      );
  }

  render() {
    return (
      <div className="service-status-panel">
        {
          this.props.services.map(function(service){
            return (
              <ServiceStatus
                key={shortid.generate()}
                status={service.status}
                name={service.name}
              />
            );
          })
        }
      </div>
    );
  }
}

ServiceStatusPanel.propTypes = propTypes;
