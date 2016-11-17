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
import {MyServiceProviderApi} from 'api/serviceproviders';
export default class ServiceStatus extends Component {

  constructor(props){
    super(props);
    this.toggleDropdownAndUpdate = this.toggleDropdownAndUpdate.bind(this);
    this.state = {
      isDropdownOpen : false,
      provisioned : this.props.numProvisioned,
      requested : this.props.numProvisioned,
      editingProvisions : false,
      serviceWarning  : false,
      showError : false,
      provisionsLoading : false,
      enteredProvisionValue : '',
      showBtnError : false,
      errorText: 'Instance count should be between [1,1]'
    };
    this.MyDataSrc = new Datasource();
    this.keyDown = this.keyDown.bind(this);
    this.toggleErrorMessage = this.toggleErrorMessage.bind(this);
    this.onProvisionChange = this.onProvisionChange.bind(this);
    this.toggleErrorBtn = this.toggleErrorBtn.bind(this);
  }

  //If the dropdown is open and the entered value is a number
  toggleDropdownAndUpdate(){

    if(this.state.isDropdownOpen && !isNaN(Number(this.state.enteredProvisionValue))){
      //Make a request and set state to loading until we receive the response
      this.setState({
        isDropdownOpen : !this.state.isDropdownOpen,
        serviceWarning : true
      });

      MyServiceProviderApi.setProvisions({
        //Place the current name in the request
        serviceid : this.props.name
      }, {
        instances : this.state.enteredProvisionValue
      }).subscribe(() => {
        this.setState({
          provisionsLoading : false,
          enteredProvisionValue : ''
        });
      });

    } else {
      //Toggle the dropdown menu
      this.setState({
        isDropdownOpen : !this.state.isDropdownOpen
      });
    }
  }

  onProvisionChange(e){
    this.setState({
      requested : e.target.value
    });
  }
  //If the user presses enter, we set the state to reflect the entered value and close the dropdown
  keyDown(e){
    if(e.keyCode !== undefined && e.keyCode === 13){
      //If there is no entered value do not set entered value
      if(e.target.value === ''){
        this.toggleDropdownAndUpdate();
      } else {
        this.setState({
          enteredProvisionValue : e.target.value
        }, () => {
          this.toggleDropdownAndUpdate();
        });
      }
    }
  }

  setProvisionNumber(){

  }

  //IF there is an error message, toggle depending on the event type
  toggleErrorMessage(e){
    if(this.state.serviceWarning){
      if(e.type === 'mouseover'){
        this.setState({
          showError : true
        });
      } else if(e.type === 'mouseout'){
        this.setState({
          showError : false
        });
      }
    }
  }
  //IF there is an error message, toggle depending on the event type
  toggleErrorBtn(e){
    if(this.state.serviceWarning){
      if(e.type === 'mouseover'){
        this.setState({
          showBtnError : true
        });
      } else if(e.type === 'mouseout'){
        this.setState({
          showBtnError : false
        });
      }
    }
  }

  render(){
    var circle = '';

    if(!this.props.isLoading){
      if(this.state.provisionsLoading){
        circle = <div className={classNames({"status-circle-green" : !this.props.isLoading, "status-circle-grey" : this.props.isLoading})}>
                  <span className="fa fa-cog fa-spin fa-fw" />
                 </div>;
      } else if(this.props.status === 'OK'){
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

    let provisionBtnClasses = classNames('btn btn-default btn-primary set-provision-btn', {'provision-btn-with-warning' : this.state.serviceWarning});

    return (
      <div
        onClick={this.toggleDropdownAndUpdate}
        className="service-status"
        onMouseOver={this.toggleErrorMessage}
        onMouseOut={this.toggleErrorMessage}
      >
        {circle}
        <div className="status-label">
          {T.translate(`features.Management.Services.${this.props.name.split('.').join('_')}`)}
          {
            this.state.serviceWarning ?
              <span
                className="fa fa-exclamation-triangle"
                aria-hidden="true"
              >
              {
                this.state.showError && !this.state.isDropdownOpen?
                <div className="service-error-message">
                  {this.state.errorText}
                </div>
                :
                null
              }
              </span>
              :
              null
          }
        </div>
        <div className="service-dropdown-container pull-right">
          <Dropdown
            isOpen={this.state.isDropdownOpen}
            toggle={this.toggleDropdownAndUpdate}
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
              <div
                className="provision-dropdown-item service-dropdown-item"
                onMouseOver={this.toggleErrorMessage}
                onMouseOut={this.toggleErrorMessage}
              >
                Provisions
                <input
                  className="provision-input"
                  autoFocus
                  placeholder={this.state.provisioned}
                  onChange={this.onProvisionChange}
                  onKeyDown={this.keyDown}
                />
                <button
                  onMouseOver={this.toggleErrorBtn}
                  onMouseOut={this.toggleErrorBtn}
                  className={provisionBtnClasses}
                  onClick={this.setProvisionNumber}
                >
                  {T.translate(`features.Management.Services.setBtn`)}
                  {
                    this.state.serviceWarning ?
                      <span
                        className="warning-triangle fa fa-exclamation-triangle"
                        aria-hidden="true"
                      >
                      {
                        this.state.showBtnError && this.state.isDropdownOpen ?
                        <div className="service-error-message-inline">
                          <div className="fa fa-caret-up" />
                          {this.state.errorText}
                        </div>
                        :
                        null
                      }
                      </span>
                      :
                      null
                  }
                </button>
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
