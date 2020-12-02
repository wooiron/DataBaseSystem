import React from 'react'
import './SearchPage.css';
import { useStateValue } from "./StateProvider";
import useGoogleSearch from './useGoogleSearch';
import { Button } from "@material-ui/core";
import { useHistory } from 'react-router-dom';
import {
  SearchBox,
  InstantSearch
} from 'react-instantsearch-dom';

function SearchPage() {
  const [{ term }, dispatch] = useStateValue();
  const { data } = useGoogleSearch(term);
  const history = useHistory();
  const searchClient = (
    'B1G2GM9NG0',
    'aadef574be1f9252bb48d4ea09b5cfe5'
  )

  const back = e => {
    e.preventDefault();
    history.push("/");
  }
  // https://developers.google.com/custom-search/v1/using_rest

  // https://cse.google.com/cse/create/new

  return (
    <div className="searchPage">

      <div className="searchPage__header">
        <h1>{term}</h1>
      </div>

      <div className="searchPage__results">
        <div className="goBack__Button">
          <Button className="goback" onClick={back}>BACK</Button>
        </div>
      </div>
    </div>
  )
}

export default SearchPage
