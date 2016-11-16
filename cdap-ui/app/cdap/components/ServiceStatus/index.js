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
import Datasource from 'services/datasource';
import {Dropdown, DropdownMenu} from 'reactstrap';
import T from 'i18n-react';

export default class ServiceStatus extends Component {

  constructor(props){
    super(props);
    this.toggleDropdown = this.toggleDropdown.bind(this);
    this.state = {
      isDropdownOpen : false,
      provisioned : this.props.numProvisioned,
      requested : this.props.numProvisioned,
      editProvisioned : false,
      serviceWarning  : false
    };
    this.MyDataSrc = new Datasource();
    this.editProvisions = this.editProvisions.bind(this);
  }
  toggleDropdown(){
    this.setState({
      isDropdownOpen : !this.state.isDropdownOpen
    });
  }
  editProvisions() {
    this.setState({
      editProvisoned : !this.state.editProvisoned
    });
  }
  keyDown(e){
    //If the enter key is pressed
    if(e.keyCode !== undefined && e.keyCode === 13){
      this.toggleDropdown();
    }
  }
  render(){
    var circle = '';

    if(!this.props.isLoading){
      if(this.props.status === 'OK'){
        circle = <div className={classNames({"status-circle-green" : !this.props.isLoading, "status-circle-grey" : this.props.isLoading})}>
                  {this.state.provisioned}
                 </div>;
      } else {
        circle = <div className={classNames({"status-circle-red" : !this.props.isLoading, "status-circle-grey" : this.props.isLoading})} />;
      }
    } else {
      circle = <div className="status-circle-grey" />;
    }

    let logUrl = this.MyDataSrc.constructUrl({
      _cdapPath : `/system/services/${this.props.name}/logs/next?&maxSize=50`
    });

    return (
      <div onClick={this.toggleDropdown} className="service-status">
        {circle}
        <div className="status-label">
          {T.translate(`features.Management.Services.${this.props.name.split('.').join('_')}`)}
          {
            this.state.serviceWarning ?
              <i className="fa fa-exclamation-triangle" aria-hidden="true"></i>  :
              null
          }
        </div>
        <div className="service-dropdown-container pull-right">
          <Dropdown
            isOpen={this.state.isDropdownOpen}
            toggle={this.toggleDropdown}
            className="service-dropdown"
          >
            <span className="fa fa-caret-down service-dropdown-caret">
            </span>
            <DropdownMenu>
              <div className="dropdown-service-name service-dropdown-item">
                {circle}
                <div className="status-label">
                  {T.translate(`features.Management.Services.${this.props.name.split('.').join('_')}`)}
                </div>
              </div>
              <a href={logUrl} target="_blank">
                <div className="service-dropdown-item">
                  <span className="fa fa-file-text" />
                    View Logs
                </div>
              </a>
              {
                this.state.editProvisioned ?
                  <div onClick={this.editProvisions} className="provision-dropdown-item service-dropdown-item">
                    Provisions
                    <input
                      className="provision-input"
                      placeholder={this.state.provisioned}
                      onChange={this.updateProvisioned}
                      onKeyDown={this.keyDown}
                    />
                  </div>
                  :
                  <div onClick={this.editProvisions} className="provision-dropdown-item service-dropdown-item">
                    Provisioned {this.state.provisioned}
                  </div>
              }
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
