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
require('./ServiceStatus.less');
var classNames = require('classnames');
import {Dropdown, DropdownMenu} from 'reactstrap';

export default class ServiceStatus extends Component {

  constructor(props){
    super(props);
    this.toggleDropdown = this.toggleDropdown.bind(this);
    this.state = {
      isDropdownOpen : false
    };
  }

  toggleDropdown(){
    this.setState({
      isDropdownOpen : !this.state.isDropdownOpen
    });
  }

  render(){
    var circle = '';

    if(!this.props.isLoading){
      if(this.props.status === 'OK'){
        circle = <div className={classNames({"status-circle-green" : !this.props.isLoading, "status-circle-grey" : this.props.isLoading})} />;
      } else {
        circle = <div className={classNames({"status-circle-red" : !this.props.isLoading, "status-circle-grey" : this.props.isLoading})} />;
      }
    } else {
      circle = <div className="status-circle-grey" />;
    }

    //To-Do: make it such that the dropdown menu does not get cut off by the container element
    return (
      <div  onClick={this.toggleDropdown} className="service-status">
        {circle}
        <div className="status-label">
          {this.props.name}
        </div>
        <div className="service-dropdown-container">
          <Dropdown
            isOpen={this.state.isDropdownOpen}
            toggle={this.toggleDropdown}
            className="service-dropdown"
          >
            <span className="fa fa-caret-down service-dropdown-caret">
            </span>
            <DropdownMenu>
              <div className="service-dropdown-item">
                Provisioned: {this.props.numProvisioned}
              </div>
            </DropdownMenu>
          </Dropdown>
        </div>
      </div>
    );
  }
}

ServiceStatus.propTypes = {
  name: PropTypes.string,
  status: PropTypes.string,
  isLoading: PropTypes.bool,
  numProvisioned: PropTypes.number
};
